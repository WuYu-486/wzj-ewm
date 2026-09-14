from flask import Flask, render_template, jsonify, request
import ad
from getdata import getData
from getSocket import TeacherMateWebSocketClient
import asyncio
import configparser
import functools
import hashlib
import hmac
import random
import time
import threading
import os
import re
import logging
import requests
import atexit
import signal
import urllib.parse
from typing import Optional, List, Dict, Any, Tuple, Set, Callable
import settings

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 可调参数 ====================
RECONCILE_INTERVAL = 5.0          # 空闲时的对账间隔（秒）
RECONCILE_INTERVAL_ACTIVE = 3.0   # 有活跃签到会话时的对账间隔（秒）
RECONCILE_INTERVAL_MAX = 20.0     # 连续失败时的最大退避间隔（秒）
ROUND_TTL_SECONDS = 20.0          # 二维码轮次有效期，超过即视为过期
FOLLOW_TIMEOUT = 10               # 跟随重定向的超时（秒）
GETDATA_TIMEOUT = 15.0            # 拉取签到列表的超时（秒）
FAYE_TIMEOUT = 15.0               # 建立 Faye 通道的超时（秒）
VANISH_STREAK_REQUIRED = 2        # 连续几次看不到才算会话消失
VANISH_RECHECK_DELAY = 30.0       # 消失后多久内若重新出现，就撤销判定
STOP_WAIT_INTERVAL = 0.25         # 可被打断睡眠的检查粒度（秒）
SWAP_WAIT_TIMEOUT = 10.0          # 换 openid 时等待旧管道停止的上限（秒）

# ==================== openid 管理 ====================
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
OPENID_PATTERN = re.compile(r"^[0-9A-Za-z_-]{16,64}$")

# 换 openid 时串行化，避免并发请求起出两个管道
_pipeline_lock = threading.Lock()
# 管理口令，服务启动时确定
admin_token: Optional[str] = None


class Pipeline:
    """签到数据管道 —— 常驻对账循环版

    与旧版的根本区别：这个管道是**常驻**的。

    老师结束签到（type==2）、WebSocket 断开、会话从签到列表消失，这些都只是
    "这一轮业务的结束"，绝不会再关闭管道本身。只有用户主动关服
    （request_stop）才会让循环退出。

    三个概念必须分清：
      - 会话 session：一次签到，标识是 signId
      - 轮次 round  ：一个二维码，标识是重定向地址里的一次性 extra 凭证（约 10 秒刷新一次）
      - 管道 pipeline：整个进程，跨会话常驻
    """

    def __init__(self, openid: str):
        self.openid = openid
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        # ---- 线程同步 ----
        self._lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self._teardown_done = False
        self._fail_count = 0

        # ---- WebSocket 客户端集合：sign_id -> {"task": Task, "client": 客户端} ----
        self.clients: Dict[int, Dict[str, Any]] = {}

        # ---- 对外发布的二维码状态 ----
        self.session_id: Optional[int] = None     # 当前签到会话
        self.session_seq = 0                      # 第几个会话
        self.round_id: Optional[str] = None       # 当前二维码轮次标识
        self.round_seq = 0                        # 第几个轮次
        self.round_updated_at = 0.0               # 轮次发布时间戳
        self.qr_url: Optional[str] = None         # 最终跳转地址
        self.qr_url_raw: Optional[str] = None     # 原始二维码地址
        self.success = 0
        self.phase = "waiting"                    # 见 get_status 的说明
        self.message: Optional[str] = None
        self.suspected_done = False
        self.is_running = False

        # ---- 会话追踪 ----
        self._closed_sign_ids: Set[int] = set()      # 收到 type==2 明确结束的会话
        self._captured_sign_ids: Set[int] = set()    # 捕获过二维码的会话
        self._vanished_streak: Dict[int, int] = {}   # 连续几次没出现在签到列表
        self._vanished_sign_ids: Set[int] = set()    # 判定消失的会话
        self._recheck_at: Dict[int, float] = {}      # 何时允许撤销"消失"判定

    # ==================== 启动与停止 ====================

    def start(self) -> None:
        """启动管道。管道一旦启动就会常驻，直到 request_stop"""
        if self.is_running:
            logger.warning("管道已经在运行中")
            return
        self.shutdown_event.clear()
        thread = threading.Thread(target=self._run_async, daemon=True, name="PipelineThread")
        thread.start()
        logger.info("管道启动完成")

    def request_stop(self) -> None:
        """请求关闭管道。线程安全，供信号处理与 atexit 调用。"""
        if self.shutdown_event.is_set():
            return
        logger.info("收到停止请求，准备关闭管道...")
        self._reset_published("收到停止请求")
        with self._lock:
            self.phase = "stopped"
            self.message = "服务已停止监听"
        self.shutdown_event.set()

    async def shutdown(self) -> None:
        """优雅关闭所有组件。幂等，重复调用无副作用。"""
        if self._teardown_done:
            return
        self._teardown_done = True
        logger.info("开始关闭管道...")
        self.shutdown_event.set()

        entries = list(self.clients.values())
        self.clients.clear()
        self.is_running = False

        if entries:
            logger.info(f"正在关闭 {len(entries)} 个 WebSocket 连接...")
            shutdown_tasks = []
            for entry in entries:
                client = entry.get("client")
                if client and not client.is_shutting_down:
                    shutdown_tasks.append(client.graceful_shutdown())
            if shutdown_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*shutdown_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                    logger.info("所有 WebSocket 连接已关闭")
                except asyncio.TimeoutError:
                    logger.warning("部分 WebSocket 连接关闭超时")
                except Exception as e:
                    logger.error(f"关闭 WebSocket 连接时出错: {e}")

            # 取消并等待客户端任务真正结束。只 cancel 不 await 的话，
            # 事件循环关闭时会报 "Task was destroyed but it is pending"。
            client_tasks = [
                entry["task"] for entry in entries
                if entry.get("task") and not entry["task"].done()
            ]
            for task in client_tasks:
                task.cancel()
            if client_tasks:
                await asyncio.gather(*client_tasks, return_exceptions=True)

        logger.info("管道关闭完成")

    def _run_async(self) -> None:
        """在新线程中运行异步主管道"""
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._main_async())
        except Exception as e:
            logger.error(f"异步运行失败: {e}")
        finally:
            if self.loop and not self.loop.is_closed():
                self.loop.close()

    # ==================== 主管道：常驻对账循环 ====================

    async def _main_async(self) -> None:
        """常驻主循环：只要没收到停止请求就一直对账

        旧版这里用的是 asyncio.wait(return_when=FIRST_COMPLETED)，只要有任意一个
        签到任务结束（例如收到老师结束签到的 type==2）就走到 finally 把整条管道
        关掉，且 is_running 再也回不到 True。这是"一次签到即报废"的根因。
        """
        self.is_running = True
        logger.info("管道已启动，开始监听签到")

        try:
            while not self.shutdown_event.is_set():
                try:
                    ok = await self._reconcile_once()
                except Exception as e:
                    # 单轮对账失败绝不能终结管道，下一轮重来
                    logger.error(f"对账失败，稍后重试: {e}")
                    ok = False

                if self.shutdown_event.is_set():
                    break

                await self._sleep_or_stop(self._next_interval(ok))
        finally:
            # 只有用户主动关服才会走到这里
            await self.shutdown()

    def _next_interval(self, ok: bool) -> float:
        """决定下一次对账的间隔，失败时指数退避"""
        if not ok:
            self._fail_count += 1
            backoff = RECONCILE_INTERVAL * (2 ** min(self._fail_count - 1, 2))
            return min(backoff, RECONCILE_INTERVAL_MAX)
        self._fail_count = 0
        return RECONCILE_INTERVAL_ACTIVE if self.clients else RECONCILE_INTERVAL

    async def _sleep_or_stop(self, seconds: float) -> None:
        """可被停止请求打断的睡眠"""
        deadline = time.monotonic() + seconds
        while not self.shutdown_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(STOP_WAIT_INTERVAL, remaining))

    async def _reconcile_once(self) -> bool:
        """拉一次签到列表，增删 WebSocket 客户端。返回本次是否成功拿到数据"""
        data, ok = await self._fetch_active_signs()
        if not ok:
            # 拉取失败时绝不做任何"会话消失"的推断，客户端保持运行
            return False

        qr_signs = {item["signId"]: item for item in data if item.get("isQR")}
        now = time.monotonic()

        # (1) 会话从签到列表消失：疑似签到成功，也可能只是活动结束
        for sign_id in list(self.clients.keys()):
            if sign_id not in qr_signs:
                await self._on_session_vanished(sign_id)

        # (2) 需要新建或需要重建的客户端
        for sign_id, item in qr_signs.items():
            if sign_id in self._closed_sign_ids:
                continue  # 老师已明确结束（type==2），不再订阅

            if sign_id in self._vanished_sign_ids:
                if now < self._recheck_at.get(sign_id, 0.0):
                    continue
                # 判定错误：会话又出现了，撤销标记重新订阅，保证不漏签
                logger.warning(f"会话 {sign_id} 重新出现在签到列表，撤销结束标记并重新订阅")
                self._vanished_sign_ids.discard(sign_id)
                self._vanished_streak.pop(sign_id, None)

            entry = self.clients.get(sign_id)
            if entry and not entry["task"].done():
                continue
            if entry:
                # 客户端静默死亡（WebSocket 断开且重连次数耗尽）。旧版没有这个检查，
                # 这种死法比 type==2 更隐蔽，会话还在列表里却再也收不到二维码。
                logger.warning(f"客户端任务已结束但会话仍在签到列表，重建: sign_id={sign_id}")
                self.clients.pop(sign_id, None)

            await self._start_client(sign_id, item["courseId"])

        return True

    async def _fetch_active_signs(self) -> Tuple[List[Dict[str, Any]], bool]:
        """拉取进行中的签到列表。返回 (列表, 是否成功)。

        成功且列表为空时返回 ([], True)，与"拉取失败"区分开。
        """
        loop = asyncio.get_running_loop()
        try:
            data = await asyncio.wait_for(
                loop.run_in_executor(None, getData, self.openid),
                timeout=GETDATA_TIMEOUT
            )
        except Exception as e:
            logger.error(f"获取签到列表失败: {e}")
            return [], False

        if isinstance(data, dict):
            # 接口返回错误信息（最常见的是 openid 失效）
            error_message = data.get("message", "未知错误")
            logger.error(f"获取签到列表出错: {error_message}")
            with self._lock:
                if self.success == 0:
                    self.phase = "error"
                    self.message = error_message
            return [], False

        if not isinstance(data, list):
            logger.warning(f"签到列表格式异常: {type(data)}")
            return [], False

        result: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if all(key in item for key in ("courseId", "signId", "isQR", "isGPS")):
                result.append({
                    "courseId": item["courseId"],
                    "signId": item["signId"],
                    "isQR": item["isQR"],
                    "isGPS": item["isGPS"],
                })
        return result, True

    # ==================== WebSocket 客户端管理 ====================

    async def _start_client(self, sign_id: int, course_id: int) -> None:
        """为一个签到会话建立 WebSocket 客户端并订阅二维码"""
        try:
            loop = asyncio.get_running_loop()
            # creatClientId 是阻塞调用（两次 HTTP 握手），必须放进线程池，
            # 否则会冻结整个事件循环，连所有客户端的心跳一起卡住
            client_id = await asyncio.wait_for(
                loop.run_in_executor(
                    None, functools.partial(ad.creatClientId, sign_id, course_id)
                ),
                timeout=FAYE_TIMEOUT
            )

            # 复位点 3：开始跟踪一个不同的会话，清掉上一个会话的残留二维码
            if self.session_id != sign_id:
                if self.session_id is not None:
                    self._reset_published(f"切换到新的签到会话 {sign_id}")
                with self._lock:
                    self.session_id = sign_id
                    self.session_seq += 1

            client = TeacherMateWebSocketClient(
                sign_id=sign_id,
                qr_callback=self._make_qr_callback(sign_id),
                event_callback=self.on_event
            )
            client.client_id = client_id

            task = asyncio.create_task(client.start())
            self.clients[sign_id] = {"task": task, "client": client}

            with self._lock:
                self.phase = "listening"
                if self.success == 0:
                    self.message = "已订阅签到，等待二维码"
            logger.info(f"已订阅签到会话: sign_id={sign_id}")

        except asyncio.TimeoutError:
            logger.error(f"建立 Faye 通道超时: sign_id={sign_id}")
        except Exception as e:
            logger.error(f"订阅签到会话失败 {sign_id}: {e}")

    async def _stop_client(self, sign_id: int) -> None:
        """停止某个会话的 WebSocket 客户端"""
        entry = self.clients.pop(sign_id, None)
        if not entry:
            return
        client = entry.get("client")
        task = entry.get("task")
        if client and not client.is_shutting_down:
            try:
                await client.graceful_shutdown()
            except Exception as e:
                logger.error(f"关闭客户端失败 {sign_id}: {e}")
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"等待客户端任务结束出错 {sign_id}: {e}")

    async def _on_session_vanished(self, sign_id: int) -> None:
        """会话从 active_signs 列表消失。

        这里用的是**未经验证的推断**：签到成功后该 signId 会从列表消失。
        为了不误判，连续 VANISH_STREAK_REQUIRED 次看不到才认定，并且过了
        VANISH_RECHECK_DELAY 后如果它又出现，会撤销判定重新订阅。
        任何情况下判错的代价都是"继续签"，绝不会漏签。
        """
        streak = self._vanished_streak.get(sign_id, 0) + 1
        self._vanished_streak[sign_id] = streak
        if streak < VANISH_STREAK_REQUIRED:
            logger.info(f"会话 {sign_id} 未出现在签到列表（第 {streak} 次），继续观察")
            return

        await self._stop_client(sign_id)
        self._vanished_sign_ids.add(sign_id)
        self._recheck_at[sign_id] = time.monotonic() + VANISH_RECHECK_DELAY
        self._vanished_streak.pop(sign_id, None)

        was_captured = sign_id in self._captured_sign_ids
        with self._lock:
            self.suspected_done = was_captured
            self.phase = "suspected_done" if was_captured else "session_closed"
            self.message = ("签到列表中已不再出现该签到，疑似签到成功（未验证推断），"
                            "继续监听下一次签到"
                            if was_captured else
                            "签到活动已结束，继续监听下一次签到")

        if was_captured:
            logger.warning(f"会话 {sign_id} 从签到列表消失，疑似签到成功（未验证推断）")
        else:
            logger.warning(f"会话 {sign_id} 从签到列表消失，且从未捕获到二维码，按活动结束处理")

        # 复位点 2
        if self.session_id == sign_id:
            self._reset_published("会话从签到列表消失")
            with self._lock:
                self.session_id = None

    # ==================== 协议事件处理 ====================

    def on_event(self, event: Dict[str, Any]) -> None:
        """处理来自 getSocket 的协议事件。

        由 WebSocket 的接收任务在事件循环线程内同步调用，必须轻量、不阻塞。
        """
        try:
            event_type = event.get("type")
            sign_id = event.get("sign_id")

            if event_type == 2:
                # 老师结束了本次签到。只结束这个会话，绝不结束管道。
                logger.info(f"会话结束(type=2): sign_id={sign_id}")
                self._closed_sign_ids.add(sign_id)
                self.clients.pop(sign_id, None)  # 客户端会自己 graceful_shutdown
                self._captured_sign_ids.discard(sign_id)

                if self.session_id == sign_id:
                    # 复位点 1
                    self._reset_published("会话结束(type=2)")
                    with self._lock:
                        self.session_id = None
                        self.phase = "session_closed"
                        self.message = "本次签到已结束，继续监听下一次签到"

            elif event_type == 3:
                # 瞬态拥挤。什么都不该停，上一轮有效二维码也保留（它可能还没过期）。
                logger.info("二维码暂不可用（前方拥挤），继续保持监听")
                with self._lock:
                    if self.success == 1:
                        self.message = "二维码暂不可用（前方拥挤），已保留上一轮有效二维码"
                    else:
                        self.phase = "listening"
                        self.message = "二维码暂不可用（前方拥挤），继续监听"

        except Exception as e:
            logger.error(f"事件处理失败: {e}")

    # ==================== 二维码轮次 ====================

    def _make_qr_callback(self, sign_id: int) -> Callable[[str], None]:
        """构造绑定 sign_id 的二维码回调。

        这是二维码进入管道的**唯一入口**。旧版同时走 result_queue 和直接调用
        两条路径，同一个二维码会被处理两遍，这里已经收敛成一条。
        """
        def _on_qr(raw_url: str) -> None:
            loop = self.loop
            if loop is None or loop.is_closed():
                logger.warning(f"事件循环未就绪，丢弃二维码: sign_id={sign_id}")
                return
            try:
                asyncio.run_coroutine_threadsafe(self._resolve_round(sign_id, raw_url), loop)
            except Exception as e:
                logger.error(f"提交二维码处理任务失败: {e}")
        return _on_qr

    async def _resolve_round(self, sign_id: int, raw_url: str) -> None:
        """跟随重定向拿到最终跳转地址，然后发布为一个新的二维码轮次"""
        if sign_id in self._closed_sign_ids:
            logger.debug(f"丢弃已结束会话的迟到二维码: sign_id={sign_id}")
            return

        loop = asyncio.get_running_loop()
        try:
            final_url = await asyncio.wait_for(
                loop.run_in_executor(None, self._follow_redirect, raw_url),
                timeout=FOLLOW_TIMEOUT + 5
            )
        except Exception as e:
            # 刻意不复位 success：上一轮二维码可能还没过期，保留它比清掉更划算
            logger.error(f"跟随重定向失败: {e}")
            with self._lock:
                if self.success == 0:
                    self.phase = "listening"
                self.message = f"获取二维码失败，继续监听: {e}"
            return

        if self.shutdown_event.is_set():
            return

        final_url = str(final_url)
        self._publish_round(sign_id, raw_url, final_url, self._make_round_id(final_url))

    @staticmethod
    def _follow_redirect(url: str) -> str:
        """跟随重定向，返回最终地址。同步方法，必须放在线程池里执行。"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x63090a13) UnifiedPCWindowsWechat(0xf2541411) XWEB/16965 Flue"
        }
        response = requests.get(url, headers=headers, timeout=FOLLOW_TIMEOUT)
        return response.url

    @staticmethod
    def _make_round_id(final_url: str) -> str:
        """计算轮次标识。

        优先取重定向地址里的一次性 extra 凭证（二维码约 10 秒刷新一次，每次
        extra 都不同，天然可以作为轮次的唯一标识）。取不到时回退为 URL 摘要。
        """
        try:
            decoded = urllib.parse.unquote(final_url)
            match = re.search(r"[?&]extra=([0-9A-Za-z_\-]+)", decoded)
            if match:
                return match.group(1)
        except Exception as e:
            logger.debug(f"解析 extra 失败，回退为 URL 摘要: {e}")
        return hashlib.sha1(final_url.encode("utf-8")).hexdigest()[:12]

    def _publish_round(self, sign_id: int, raw_url: str, final_url: str, round_id: str) -> None:
        """发布一个新的二维码轮次"""
        with self._lock:
            # 源级去重：服务端重复推同一个二维码时，什么都不做
            if round_id == self.round_id:
                logger.debug(f"重复的二维码，忽略: {round_id[:12]}")
                return
            if sign_id in self._closed_sign_ids:
                return

            self.round_seq += 1
            self.session_id = sign_id
            self.round_id = round_id
            self.qr_url_raw = raw_url
            self.qr_url = final_url
            self.round_updated_at = time.time()
            self.success = 1
            self.suspected_done = False
            self.phase = "captured"
            self.message = f"已捕获第 {self.round_seq} 个二维码"
            self._captured_sign_ids.add(sign_id)

        logger.info(f"发布二维码轮次 #{self.round_seq} session={sign_id} round={round_id[:12]}")

    def _reset_published(self, reason: str) -> None:
        """复位对外发布的二维码状态。

        旧版 success 一旦置 1 就再也没被改回 0，导致前端永远跳向那个早已结束的
        签到页。这里是那个 bug 的修复点。
        """
        with self._lock:
            if self.success == 1 or self.round_id is not None:
                logger.info(f"复位二维码状态，原因: {reason}")
            self.success = 0
            self.qr_url = None
            self.qr_url_raw = None
            self.round_id = None
            self.round_updated_at = 0.0

    # ==================== 对外状态 ====================

    def get_status(self) -> Dict[str, Any]:
        """当前状态。

        这是**纯读**方法，没有任何副作用。旧版会在这里从 result_queue 里把
        二维码 URL 取出来再处理一遍，导致同一个二维码被处理两次。
        """
        with self._lock:
            age_ms: Optional[int] = None
            if self.round_updated_at:
                age_ms = int((time.time() - self.round_updated_at) * 1000)
            stale = bool(age_ms is not None and age_ms > ROUND_TTL_SECONDS * 1000)
            fresh = bool(self.success == 1 and not stale)

            return {
                # success / message / qr_url 保留旧字段名以兼容前端
                "success": 1 if fresh else 0,
                "message": self.message,
                "qr_url": self.qr_url,
                "qr_url_raw": self.qr_url_raw,
                # 前端改用 round_id 判重，不再依赖 success
                "round_id": self.round_id,
                "round_seq": self.round_seq,
                "age_ms": age_ms,
                "stale": stale,
                "session_id": self.session_id,
                "session_seq": self.session_seq,
                "suspected_done": self.suspected_done,
                # phase 取值：waiting / listening / captured /
                #             session_closed / suspected_done / error / stopped
                "phase": self.phase,
                "pipeline_running": self.is_running,
                "server_time": int(time.time() * 1000),
            }


# 全局变量
openid = os.getenv("OPENID")
app = Flask(__name__)
pipeline: Optional[Pipeline] = None


def create_pipeline() -> Optional[Pipeline]:
    """创建并启动管道"""
    global pipeline
    if not openid:
        logger.error("未找到OPENID环境变量")
        return None

    pipeline = Pipeline(openid)
    pipeline.start()
    logger.info("管道创建并启动成功")
    return pipeline


# ==================== openid 管理 ====================

def _read_config() -> configparser.ConfigParser:
    """读取 config.ini，文件不存在时返回空配置"""
    config = configparser.ConfigParser()
    try:
        config.read(CONFIG_PATH, encoding="utf-8")
    except Exception as e:
        logger.error(f"读取 config.ini 失败: {e}")
    return config


def _write_config(config: configparser.ConfigParser) -> None:
    """把配置写回 config.ini"""
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as file:
            config.write(file)
    except Exception as e:
        logger.error(f"写入 config.ini 失败: {e}")


def _load_or_create_admin_token() -> str:
    """读取管理口令，没有就生成一个并写进 config.ini 的 [admin] 段。

    这个口令用来保护「更换 openid」接口。服务必须监听局域网才能让手机访问
    （openid 本来就是从手机微信里复制出来的），不加保护的话同网段任何人都能
    打开页面把 openid 换掉。
    """
    global admin_token

    config = _read_config()
    if config.has_section("admin") and config.has_option("admin", "token"):
        token = config.get("admin", "token").strip()
        if token:
            admin_token = token
            return token

    token = "%08x" % random.getrandbits(32)
    if not config.has_section("admin"):
        config.add_section("admin")
    config.set("admin", "token", token)
    _write_config(config)
    admin_token = token
    logger.info("已生成新的管理口令并写入 config.ini")
    return token


def _extract_openid(text: str) -> Optional[str]:
    """从链接或裸字符串里提取 openid

    兼容两种情况：
      - 整条链接：https://v18.teachermate.cn/wechat-pro-ssr/?openid=xxxx&from=wzj
      - 只粘贴了裸的 openid 字符串
    """
    if not text:
        return None
    text = text.strip()

    match = re.search(r"[?&]openid=([0-9A-Za-z_\-]+)", text)
    if match:
        return match.group(1)

    if OPENID_PATTERN.match(text):
        return text
    return None


def _mask_openid(value: Optional[str]) -> Optional[str]:
    """脱敏显示：只露首尾各 4 位"""
    if not value:
        return None
    if len(value) <= 8:
        return value[:2] + "*" * max(len(value) - 2, 0)
    return f"{value[:4]}****{value[-4:]}"


def _verify_openid(candidate: str) -> Tuple[bool, str]:
    """调一次接口验证 openid 是否可用。返回 (是否通过, 说明)。

    拿不准的时候一律放行：管道本身会退避重试，没必要因为一次网络抖动就把用户
    挡在门外。
    """
    try:
        data = getData(candidate)
    except Exception as e:
        logger.warning(f"验证 openid 时请求失败，直接放行: {e}")
        return True, "无法连接接口验证，已直接采用（管道会自己重试）"

    if isinstance(data, dict):
        return False, data.get("message", "该 openid 无效")
    return True, "验证通过"


def _save_openid(value: str) -> None:
    """把 openid 写回 config.ini，这样进程重启后不用重新粘贴"""
    config = _read_config()
    if not config.has_section("user"):
        config.add_section("user")
    config.set("user", "openid", value)
    _write_config(config)


def _swap_pipeline(new_openid: str) -> None:
    """停掉旧管道，用新 openid 起一条新管道。

    调用方必须持有 _pipeline_lock，否则并发请求会起出两个管道。
    """
    global pipeline, openid

    old = pipeline
    if old is not None:
        old.request_stop()
        deadline = time.monotonic() + SWAP_WAIT_TIMEOUT
        while old.is_running and time.monotonic() < deadline:
            time.sleep(0.05)
        if old.is_running:
            logger.warning(f"旧管道未能在 {SWAP_WAIT_TIMEOUT} 秒内停止，仍继续切换")

    openid = new_openid
    os.environ["OPENID"] = new_openid
    _save_openid(new_openid)

    # 必须新建实例：Pipeline 的 _teardown_done 是一次性的，旧对象无法复用
    pipeline = Pipeline(new_openid)
    pipeline.start()
    logger.info(f"已切换到新的 openid（{_mask_openid(new_openid)}）")


def _stop_current_pipeline() -> None:
    """停止当前管道，供 atexit 调用。

    不能直接注册某个 Pipeline 实例的方法：换过 openid 之后，被注册的会是早就
    废弃的旧对象。
    """
    current = pipeline
    if current is not None:
        current.request_stop()


@app.after_request
def add_header(response):
    """
    添加头部信息禁止缓存
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/qr_code')
def qr_code():
    if pipeline is None:
        return jsonify({
            "success": 0,
            "message": "管道未初始化"
        })

    status = pipeline.get_status()
    return jsonify(status)


@app.route('/health')
def health():
    """健康检查端点"""
    if pipeline is None:
        return jsonify({"status": "error", "message": "管道未初始化"}), 500

    # 注意：pipeline_running 在老师结束签到之后仍然应该是 true。
    # 一次签到不再让管道报废，这是本次修复的核心验收点。
    return jsonify({
        "status": "healthy",
        "pipeline_running": pipeline.is_running,
        "success": pipeline.get_status()["success"],
        "phase": pipeline.phase,
        "round_seq": pipeline.round_seq,
        "session_id": pipeline.session_id,
        "suspected_done": pipeline.suspected_done,
    })


@app.route('/openid', methods=['GET'])
def get_openid():
    """返回当前 openid 的脱敏信息。

    完整 openid 只在后端内存里，绝不下发到前端。
    """
    value = pipeline.openid if pipeline is not None else openid
    return jsonify({
        "configured": bool(value),
        "openid_masked": _mask_openid(value),
        "openid_tail": value[-4:] if value else None,
    })


@app.route('/openid', methods=['POST'])
def set_openid():
    """提交含 openid 的链接（或裸 openid），校验口令后热替换管道"""
    body = request.get_json(silent=True) or {}
    token = str(body.get("token", ""))
    link = str(body.get("link", ""))

    # 1) 口令校验
    expected = admin_token or ""
    if not expected or not token or not hmac.compare_digest(token, expected):
        logger.warning(f"更换 openid 的口令校验失败，来源: {request.remote_addr}")
        return jsonify({"success": False, "message": "管理口令不正确"}), 401

    # 2) 提取 openid
    candidate = _extract_openid(link)
    if not candidate:
        return jsonify({
            "success": False,
            "message": "没能从输入里识别出 openid，请粘贴完整链接或 openid 本身"
        }), 400

    # 3) 验证。这一步不通过就绝不碰旧管道，保证换错了也不会把正在用的弄丢
    ok, reason = _verify_openid(candidate)
    if not ok:
        logger.warning(f"提交的 openid 校验失败: {reason}")
        return jsonify({"success": False, "message": f"这个 openid 不可用：{reason}"}), 400

    # 4) 热替换
    try:
        with _pipeline_lock:
            _swap_pipeline(candidate)
    except Exception as e:
        logger.error(f"切换 openid 失败: {e}")
        return jsonify({"success": False, "message": f"切换失败：{e}"}), 500

    logger.info("openid 更换成功")
    return jsonify({
        "success": True,
        "message": f"已切换到 {_mask_openid(candidate)}（{reason}）",
        "openid_masked": _mask_openid(candidate),
        "openid_tail": candidate[-4:],
    })


if __name__ == '__main__':
    # 管理口令：没有就生成一个，并打印在控制台上，方便从手机来换 openid 时查
    token = _load_or_create_admin_token()
    print("\n" + "=" * 52, flush=True)
    print(f"  管理口令: {token}", flush=True)
    print("  在网页上更换 openid 时需要填这个口令", flush=True)
    print("=" * 52 + "\n", flush=True)

    # 创建并启动管道
    create_pipeline()

    # 进程退出时请求管道优雅关闭
    atexit.register(_stop_current_pipeline)

    def _signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，准备关闭应用...")
        _stop_current_pipeline()

    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _signal_handler)

    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # 生产环境设置为False
        threaded=True
    )
