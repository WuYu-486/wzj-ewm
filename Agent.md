# Agent.md — 项目记忆

## Python 环境

本机检测到两个 Python，本项目使用 **PATH 上的默认 Python 3.10.11**：

| 项目 | 路径 |
| --- | --- |
| **本项目使用的 Python** | `C:\Users\24409\AppData\Local\Microsoft\WindowsApps\PythonSoftwareFoundation.Python.3.10_qbz5n2kfra8p0\python.exe` |
| 调用方式 | 命令行直接输入 `python`（`where python` 解析到 WindowsApps 别名，实际指向上面这个） |
| 版本 | Python 3.10.11 |
| 备选 Python（未使用） | `C:\Users\24409\AppData\Local\Programs\Python\Python313\python.exe`（Python 3.13，仅装了 pip） |

pip 安装依赖请统一使用：

```bash
python -m pip install <包名>
```

## 项目结构

```
weiClass-master/
├── web.py          # 网页版入口（Flask，推荐）
├── run.py          # 图形版入口（wxPython 桌面窗口）
├── gui.py          # wxPython 界面实现
├── getSocket.py    # WebSocket 客户端，订阅签到通道拿二维码
├── getdata.py      # 调用微助教接口查询进行中的签到
├── ad.py           # 建立 Faye 通道，获取 clientId
├── settings.py     # 读取 config.ini，写入 OPENID 环境变量
├── config.ini      # 存放 openid（有效期约 2 小时）与管理口令
├── templates/index.html   # 网页版前端页面
└── tools/
    ├── offline_check.py   # 离线状态机仿真，验证管道逻辑
    └── openid_check.py    # 验证 openid 更换接口
```

`docs/` 目录在项目根（`wzj-ewm/docs/`），记录优化工作。

## 启动方式

```bash
# 网页版（默认推荐），监听 0.0.0.0:5000
cd weiClass-master
PYTHONIOENCODING=utf-8 python web.py

# 图形版（需要 wxPython）
python run.py
```

启动时控制台会打印一行**管理口令**（例如 `管理口令: 1a2b3c4d`），
在网页上更换 openid 时要填它。

## 网页上更换 openid

不用再改 config.ini、也不用重启进程：

1. 浏览器打开 `http://127.0.0.1:5000`（或局域网地址）
2. 在「openid 设置」区粘贴含 openid 的完整链接，例如
   `https://v18.teachermate.cn/wechat-pro-ssr/?openid=xxxx&from=wzj`
3. 填入控制台打印的管理口令，点「更换」

也可以只粘贴裸的 openid 字符串。更换成功后立即生效并写回 `config.ini`。
页面上 openid 只做脱敏显示（首 4 位 + 末 4 位），完整值不下发到前端。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `GET /openid` | 返回当前 openid 的脱敏值与是否已配置 |
| `POST /openid` | 提交 `{"link": "...", "token": "..."}` 更换 openid |

管理口令存在 `config.ini` 的 `[admin] token`，首次启动自动生成。

## 依赖

| 依赖 | 用途 | 运行环境 | 本机状态 |
| --- | --- | --- | --- |
| requests | 请求微助教接口 | 两者都要 | 已装 2.34.2 |
| websockets | 订阅签到通道 | 两者都要 | 已装 15.0.1（`WebSocketClientProtocol` 有弃用告警，但不影响运行） |
| flask | 网页版服务 | 仅 web.py | 已装 3.1.3 |
| wxPython | 桌面窗口 | 仅 run.py | **未装**（当前只跑网页版） |
| qrcode / Pillow | 生成二维码图片 | 仅 run.py | **未装**（当前只跑网页版） |

当前决定：只运行网页版 `web.py`，桌面版依赖暂不安装。

## 关键限制

- openid 从微信打开微助教后的网址里复制，**有效期约 2 小时**，重新打开微助教会刷新
- openid 失效时页面会高亮提示，直接粘新链接更换即可，管道会继续重试不会退出
- 签到二维码的跳转链接**只能在微信内置浏览器中打开**
- 一次签到会话中二维码约 **10 秒刷新一次**，每次都是独立的一次性凭证
- config.ini 里的 openid 与管理口令都属于敏感信息，**不要提交到 Git**（建议加进 .gitignore）
- 服务监听 `0.0.0.0`，同局域网可访问；更换 openid 的接口靠管理口令保护

## 验证脚本

```bash
cd weiClass-master
python tools/offline_check.py   # 管道状态机仿真，不需要真实签到
python tools/openid_check.py    # openid 更换接口，用临时 config 不碰真文件
```
