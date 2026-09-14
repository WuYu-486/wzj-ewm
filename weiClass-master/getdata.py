import requests
import json

def getData(openid):
    url  = "https://v18.teachermate.cn/wechat-api/v1/class-attendance/student/active_signs"
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        "Openid": openid}
    # 加超时：这个方法会被放进线程池执行，挂死的请求会长期占用线程池配额
    response = requests.get(url, headers=headers, timeout=10)
    data = json.loads(response.text)
    return data
