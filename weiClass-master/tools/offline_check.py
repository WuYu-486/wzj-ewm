"""离线状态机仿真 —— 在没有真实签到的情况下验证 web.py 的管道逻辑。

它把三处外部依赖换掉，让整个管道可以完全离线跑：
  - web.getData            : 签到列表由本脚本控制
  - web.ad.creatClientId   : 直接返回假的 clientId，不请求 Faye
  - TeacherMateWebSocketClient : 换成假客户端，二维码由脚本手工推送
  - Pipeline._follow_redirect  : 直接拼出带 extra 的地址，不发 HTTP

用法（在 weiClass-master 目录下）：
    python tools/offline_check.py

判定标准：所有剧本通过，且**整个流程跑完后 pipeline_running 仍为 true**。
旧版在这里会变成 false —— 那正是"一次签到即报废"的根因。
"""

import asyncio
import os
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# 让脚本可以从任意位置运行
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import web  # noqa: E402

# ==================== 可控的假数据源 ====================

FAKE_SIGNS = []
FAKE_RAISE = False


def fake_get_data(openid):
    """替代 getData：签到列表由脚本控制"""
    if FAKE_RAISE:
        raise RuntimeError("模拟网络故障")
    return list(FAKE_SIGNS)


def fake_creat_client_id(sign_id, courseId):
    """替代 ad.creatClientId：不请求 Faye"""
    return f"fake-client-{sign_id}"


def fake_follow_redirect(url):
    """替代跟随重定向：把 URL 末尾那一段当作一次性 extra 凭证"""
    token = url.rsplit('/', 1)[-1]
    return (
        "https://open.weixin.qq.com/connect/oauth2/authorize?appid=fake"
        "&redirect_uri=https%3A%2F%2Fwww.teachermate.com.cn%2Fapi%2Fv1%2Fwechat"
        f"%2Fr%3Fextra%3D{token}&response_type=code"
    )


def sign(sign_id, course_id):
    return {"courseId": course_id, "signId": sign_id, "isQR": True, "isGPS": False}


class FakeClient:
    """替代 TeacherMateWebSocketClient：记录回调，二维码由脚本推送"""

    instances = []

    def __init__(self, sign_id, qr_callback=None, event_callback=None):
        self.sign_id = sign_id
        self.qr_callback = qr_callback
        self.event_callback = event_callback
        self.client_id = ""
        self.is_shutting_down = False
        FakeClient.instances.append(self)

    async def start(self):
        while not self.is_shutting_down:
            await asyncio.sleep(0.05)

    async def graceful_shutdown(self):
        self.is_shutting_down = True

    def push_qr(self, token):
        """模拟收到 type==1 的二维码消息"""
        if self.qr_callback:
            self.qr_callback(f"https://www.teachermate.com.cn/api/v1/qr/attendance/{token}")

    def push_event(self, event_type):
        """模拟收到协议事件。

        必须和真实客户端一致：先把事件上报给管道，type==2 时再自毁。
        真实 getSocket 的顺序就是「先上报，再 graceful_shutdown」。
        """
        if self.event_callback:
            self.event_callback({"type": event_type, "sign_id": self.sign_id})
        if event_type == 2:
            self.is_shutting_down = True


# ==================== 测试辅助 ====================

RESULTS = []


def check(desc, actual, expected):
    ok = actual == expected
    RESULTS.append(ok)
    mark = "OK  " if ok else "FAIL"
    suffix = "" if ok else f"   <-- 期望 {expected!r}"
    print(f"    [{mark}] {desc}: {actual!r}{suffix}")
    return ok


def wait_until(predicate, desc, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    RESULTS.append(False)
    print(f"    [FAIL] 等待超时: {desc}")
    return False


def latest_client(sign_id):
    for client in reversed(FakeClient.instances):
        if client.sign_id == sign_id:
            return client
    return None


# ==================== 剧本 ====================

def run():
    pipeline = web.Pipeline("fake-openid")
    pipeline.start()

    if not wait_until(lambda: pipeline.is_running, "管道启动"):
        return

    print("\n剧本 1 · 出现一个二维码签到会话，自动订阅")
    FAKE_SIGNS[:] = [sign(1001, 1)]
    if not wait_until(lambda: latest_client(1001) is not None, "订阅 signId=1001"):
        return
    client = latest_client(1001)
    check("已订阅会话", 1001 in pipeline.clients, True)

    print("\n剧本 2 · 捕获二维码，发布第一个轮次")
    client.push_qr("TOKEN001")
    wait_until(lambda: pipeline.get_status()["success"] == 1, "发布轮次")
    status = pipeline.get_status()
    check("round_seq", status["round_seq"], 1)
    check("round_id", status["round_id"], "TOKEN001")
    check("session_id", status["session_id"], 1001)
    check("phase", status["phase"], "captured")

    print("\n剧本 3 · 二维码刷新，产生新轮次")
    client.push_qr("TOKEN002")
    wait_until(lambda: pipeline.get_status()["round_id"] == "TOKEN002", "轮次刷新")
    check("round_seq 递增", pipeline.get_status()["round_seq"], 2)

    print("\n剧本 4 · 重复推同一个二维码，源级去重")
    client.push_qr("TOKEN002")
    time.sleep(0.6)
    status = pipeline.get_status()
    check("round_id 不变", status["round_id"], "TOKEN002")
    check("round_seq 不增", status["round_seq"], 2)

    print("\n剧本 5 · type==3 前方拥挤：不结束、不复位")
    client.push_event(3)
    time.sleep(0.3)
    status = pipeline.get_status()
    check("success 保持", status["success"], 1)
    check("round_id 保留", status["round_id"], "TOKEN002")

    print("\n剧本 6 · type==2 会话结束：复位状态，但管道必须活着")
    client.push_event(2)
    wait_until(lambda: pipeline.get_status()["success"] == 0, "状态复位")
    status = pipeline.get_status()
    check("success 已复位", status["success"], 0)
    check("round_id 已清空", status["round_id"], None)
    check("phase", status["phase"], "session_closed")
    check("pipeline_running 仍为 true（核心断言）", pipeline.is_running, True)

    print("\n剧本 7 · 老师发起新签到，管道自动捕获")
    FAKE_SIGNS[:] = [sign(1001, 1), sign(1002, 2)]
    if not wait_until(lambda: latest_client(1002) is not None, "订阅 signId=1002"):
        return
    client2 = latest_client(1002)
    client2.push_qr("TOKEN003")
    wait_until(lambda: pipeline.get_status()["success"] == 1, "发布新会话轮次")
    status = pipeline.get_status()
    check("session_id 已切换", status["session_id"], 1002)
    check("phase", status["phase"], "captured")
    check("pipeline_running 仍为 true", pipeline.is_running, True)

    print("\n剧本 8 · 会话从签到列表消失（连续 2 次）：疑似签到成功")
    FAKE_SIGNS[:] = [sign(1001, 1)]
    if not wait_until(lambda: pipeline.get_status()["suspected_done"], "疑似签到成功判定"):
        return
    status = pipeline.get_status()
    check("suspected_done", status["suspected_done"], True)
    check("success 已复位", status["success"], 0)
    check("phase", status["phase"], "suspected_done")
    check("pipeline_running 仍为 true", pipeline.is_running, True)

    print("\n剧本 9 · 判定错了：会话重新出现，撤销判定并重新订阅")
    FAKE_SIGNS[:] = [sign(1001, 1), sign(1002, 2)]
    if not wait_until(
        lambda: 1002 in pipeline.clients and latest_client(1002) is not None
        and not latest_client(1002).is_shutting_down,
        "重新订阅 signId=1002"
    ):
        return
    client3 = latest_client(1002)
    client3.push_qr("TOKEN004")
    wait_until(lambda: pipeline.get_status()["success"] == 1, "重新捕获二维码")
    status = pipeline.get_status()
    check("success 恢复为 1", status["success"], 1)
    check("suspected_done 已清除", status["suspected_done"], False)

    print("\n剧本 10 · getData 抛异常：不做任何推断，管道存活")
    global FAKE_RAISE
    FAKE_RAISE = True
    time.sleep(2.0)
    check("pipeline_running 仍为 true", pipeline.is_running, True)
    check("异常期间不误判为签到成功", pipeline.get_status()["suspected_done"], False)
    FAKE_RAISE = False

    print("\n剧本 11 · 用户主动关服：管道正常退出")
    pipeline.request_stop()
    wait_until(lambda: pipeline.is_running is False, "管道停止")
    check("停止后 pipeline_running 为 false", pipeline.is_running, False)


def main():
    # 缩短各种间隔，让仿真跑得快一些
    web.RECONCILE_INTERVAL = 0.5
    web.RECONCILE_INTERVAL_ACTIVE = 0.5
    web.RECONCILE_INTERVAL_MAX = 1.0
    web.VANISH_RECHECK_DELAY = 1.5

    # 替换外部依赖
    web.getData = fake_get_data
    web.ad.creatClientId = fake_creat_client_id
    web.TeacherMateWebSocketClient = FakeClient
    web.Pipeline._follow_redirect = staticmethod(fake_follow_redirect)

    print("=" * 60)
    print("离线状态机仿真")
    print("=" * 60)

    try:
        run()
    except Exception as e:
        import traceback
        traceback.print_exc()
        RESULTS.append(False)

    print("\n" + "=" * 60)
    passed = sum(1 for r in RESULTS if r)
    total = len(RESULTS)
    print(f"结果: {passed}/{total} 通过")
    print("=" * 60)
    return 0 if passed == total and total > 0 else 1


if __name__ == '__main__':
    sys.exit(main())
