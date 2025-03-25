import requests

# 钉钉机器人的 access_token
ACCESS_TOKEN = "174034fa0df4668c5363a71a420f68a76ca2c4dde4bc25672a5f35a808e84ed8"

# 被@用户的手机号码列表
at_mobiles = ["18820132137"]

# ActionCard 内容
action_card_content = {
    "btnOrientation": "1",
    "btns": [
        {
            "actionURL": "dingtalk://dingtalkclient/page/link?pc_slide=false&url=http%3A%2F%2F127.0.0.1%3A8080%2Fcat%2Fs%2Fconfig%3Fop%3DexceptionThresholdUpdate%26domain%3Deden-demo-cola%26exception%3DTotal",
            "title": "🔧 告警规则"
        },
        {
            "actionURL": "dingtalk://dingtalkclient/page/link?pc_slide=false&url=http%3A%2F%2F127.0.0.1%3A8080%2Fcat%2Fr%2Fe%3Fdomain%3Deden-demo-cola%26date%3D2024041620%26ip%3DAll%26type%3DRuntimeException%2CException%26metric%3D",
            "title": "🔔 查看告警"
        }
    ],
    "text": "### <font color=\"#B5BB4E\">⚠️ 系统预警：eden-demo-cola</font>\n\n告警时间：2024-04-16 20:01:00\n\n\r\n告警类型：Exception\n\n\r\n告警指标：Total\n\n\r\n告警内容：当前值=3，阈值=1\n\n错误分布：\n\n172.26.224.1=3\n\n错误信息：\n\norg.ylzl.eden.spring.framework.error.ClientException=3\r\n\n\n推送间隔：1分钟\n\n所属部门：应用开发部\n\n所属产品：演示工程\n\n负责人员：郭远陆\n\n联系号码：@18820132137",
    "title": "⚠️ 系统预警：eden-demo-cola"
}

# 构造完整的消息体
message = {
    "msgtype": "actionCard",
    "actionCard": action_card_content,
    "at": {
        "atMobiles": at_mobiles,
        "isAtAll": True,
    },
}

# 发送请求
url = f"https://oapi.dingtalk.com/robot/send?access_token={ACCESS_TOKEN}"
response = requests.post(url, json=message)

# 检查响应状态码
if response.status_code == 200:
    print("消息发送成功")
else:
    print("消息发送失败，响应状态码：", response.status_code)
