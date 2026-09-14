"""openid 更换接口的验证脚本 —— 用 Flask 的 test_client 打接口，不起真实服务。

用临时目录里的 config.ini，所以**不会动到项目里真正的 config.ini**。

用法（在 weiClass-master 目录下）：
    python tools/openid_check.py

重点看两件事：
  1. 口令错误 / 输入无效 / openid 校验失败时，**旧管道必须原封不动**，绝不能换坏。
  2. 更换成功之后管道仍在运行，且 config.ini 被正确写回。
"""

import os
import shutil
import sys
import tempfile
import time

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import web  # noqa: E402

RESULTS = []

# 测试用的假 openid（格式合法，32 位十六进制）
OLD_OPENID = "aaaa1111bbbb2222cccc3333dddd4444"
NEW_OPENID = "eeee5555ffff6666aaaa7777bbbb8888"
TEST_TOKEN = "testtoken"


def check(desc, actual, expected):
    ok = actual == expected
    RESULTS.append(ok)
    mark = "OK  " if ok else "FAIL"
    suffix = "" if ok else f"   <-- 期望 {expected!r}"
    print(f"    [{mark}] {desc}: {actual!r}{suffix}")
    return ok


def main():
    # ---- 用临时的 config.ini，不碰项目的真文件 ----
    tmpdir = tempfile.mkdtemp(prefix="openid_check_")
    config_path = os.path.join(tmpdir, "config.ini")
    with open(config_path, "w", encoding="utf-8") as file:
        file.write(
            "[user]\n"
            f"openid = {OLD_OPENID}\n"
            "\n"
            "[admin]\n"
            f"token = {TEST_TOKEN}\n"
        )
    web.CONFIG_PATH = config_path

    # ---- 缩短间隔 + 屏蔽网络 ----
    web.RECONCILE_INTERVAL = 0.2
    web.RECONCILE_INTERVAL_ACTIVE = 0.2
    web.RECONCILE_INTERVAL_MAX = 0.5
    web.SWAP_WAIT_TIMEOUT = 5.0

    state = {"data": []}
    web.getData = lambda openid: state["data"]

    print("=" * 60)
    print("openid 更换接口验证")
    print("=" * 60)

    print("\n准备 · 口令读取与管道启动")
    token = web._load_or_create_admin_token()
    check("从 config.ini 读出口令", token, TEST_TOKEN)
    web.openid = OLD_OPENID
    web.create_pipeline()
    if not _wait(lambda: web.pipeline.is_running):
        check("管道启动", False, True)
        return _summary()
    check("管道已启动", web.pipeline.is_running, True)

    client = web.app.test_client()

    print("\n用例 1 · GET /openid 只回脱敏值")
    resp = client.get("/openid")
    body = resp.get_json()
    check("状态码", resp.status_code, 200)
    check("脱敏值", body["openid_masked"], "aaaa****4444")
    check("响应中不含完整 openid", OLD_OPENID in resp.get_data(as_text=True), False)

    print("\n用例 2 · 不带口令提交")
    before = web.pipeline
    resp = client.post("/openid", json={"link": f"?openid={NEW_OPENID}"})
    check("状态码", resp.status_code, 401)
    check("旧管道没被换掉", web.pipeline is before, True)

    print("\n用例 3 · 口令错误")
    resp = client.post("/openid", json={"link": f"?openid={NEW_OPENID}", "token": "wrong"})
    check("状态码", resp.status_code, 401)
    check("旧管道没被换掉", web.pipeline is before, True)

    print("\n用例 4 · 输入里没有 openid")
    resp = client.post("/openid", json={"link": "这是一段无关文字", "token": TEST_TOKEN})
    check("状态码", resp.status_code, 400)
    check("旧管道没被换掉", web.pipeline is before, True)

    print("\n用例 5 · openid 校验不通过（接口返回错误信息）")
    state["data"] = {"message": "登录信息失效，请退出后重试"}
    resp = client.post("/openid", json={"link": f"?openid={NEW_OPENID}", "token": TEST_TOKEN})
    check("状态码", resp.status_code, 400)
    check("报错原文已透传", "登录信息失效" in resp.get_json()["message"], True)
    check("旧管道没被换掉", web.pipeline is before, True)

    print("\n用例 6 · 校验通过，粘贴整条链接更换")
    state["data"] = []
    link = f"https://v18.teachermate.cn/wechat-pro-ssr/?openid={NEW_OPENID}&from=wzj"
    resp = client.post("/openid", json={"link": link, "token": TEST_TOKEN})
    body = resp.get_json()
    check("状态码", resp.status_code, 200)
    check("成功标志", body["success"], True)
    check("脱敏值", body["openid_masked"], "eeee****8888")
    check("管道已换成新对象", web.pipeline is not before, True)
    check("全局 openid 已更新", web.openid, NEW_OPENID)

    print("\n用例 7 · 更换过程中没有重启，管道仍在运行")
    _wait(lambda: web.pipeline.is_running)
    health = client.get("/health").get_json()
    check("pipeline_running", health["pipeline_running"], True)
    check("session_id 已清零", health["session_id"], None)
    check("phase 回到 waiting", health["phase"], "waiting")

    print("\n用例 8 · 只粘贴裸 openid 也能换")
    bare = "1234567890abcdef1234567890abcdef"
    resp = client.post("/openid", json={"link": bare, "token": TEST_TOKEN})
    check("状态码", resp.status_code, 200)
    check("全局 openid 已更新", web.openid, bare)

    print("\n用例 9 · config.ini 被正确写回，其余段落未被破坏")
    import configparser
    saved = configparser.ConfigParser()
    saved.read(config_path, encoding="utf-8")
    check("[user] openid 是新值", saved.get("user", "openid"), bare)
    check("[admin] token 未被破坏", saved.get("admin", "token"), TEST_TOKEN)

    print("\n用例 10 · 口令缺失时生成新口令并落盘")
    with open(config_path, "w", encoding="utf-8") as file:
        file.write(f"[user]\nopenid = {OLD_OPENID}\n")
    generated = web._load_or_create_admin_token()
    check("生成了 8 位口令", len(generated), 8)
    check("口令已落盘", web._read_config().get("admin", "token"), generated)

    print("\n用例 11 · 结束前正常关停")
    web._stop_current_pipeline()
    _wait(lambda: web.pipeline.is_running is False)
    check("管道已停止", web.pipeline.is_running, False)

    shutil.rmtree(tmpdir, ignore_errors=True)
    return _summary()


def _wait(predicate, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(0.05)
    return False


def _summary():
    print("\n" + "=" * 60)
    passed = sum(1 for r in RESULTS if r)
    total = len(RESULTS)
    print(f"结果: {passed}/{total} 通过")
    print("=" * 60)
    return 0 if passed == total and total > 0 else 1


if __name__ == '__main__':
    sys.exit(main())
