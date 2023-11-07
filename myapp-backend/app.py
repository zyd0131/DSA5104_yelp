# 导入 Flask
import json
from flask import Flask, jsonify
# 导入 CORS
from flask_cors import CORS
# 导入 User 类
from mysql import MySQL
from mongodb import MongoDB
from settings import *
from bson import json_util

"""
接口说明：
1.返回的是json数据
2.结构如下
{
    resCode： 0, # 非0即错误 1
    data： # 数据位置，一般为数组
    message： '对本次请求的说明'
}
"""

# 创建一个 Flask 应用实例
app = Flask(__name__)
# 并允许来自所有域的请求
# CORS(app)

app.config['JSON_AS_ASCII'] = False  # jsonify返回的中文正常显示

# 定义一个简单的路由
@app.route('/')
def test():
    # user = MySQL()
    # data = user.get_all_user()
    test = MongoDB()
    # data = test.get_all("test0")
    data = test.get_data("test0", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    print(data)
    return json.loads(json_util.dumps(data))
    # return jsonify(data)



@app.route('/address0', methods=['GET'])
def get_address0():
    # 创建一个字典作为模拟数据
    resData = {
        "resCode": 0,  # 非0即错误 1
        "data": [
                {"id": 0, "text": 'a', "url": '/'},
                {"id": 1, "text": 'b', "url": '/b'},
                {"id": 2, "text": 'c', "url": '/c'},
                {"id": 3, "text": 'd', "url": '/d'},
                {"id": 4, "text": 'e', "url": '/e'},
                {"id": 5, "text": 'f', "url": '/f'},
                {"id": 6, "text": 'g', "url": '/g'},
                {"id": 7, "text": 'h', "url": '/h'},
                {"id": 8, "text": 'i', "url": '/i'},
                {"id": 9, "text": 'j', "url": '/j'},
        ], # 数据位置，一般为数组
        "message": '对本次请求的说明'
    }
    # 将字典转为 JSON 并返回
    return jsonify(resData)

# app.route('/address1', methods=['GET'])
# def get_address1():
#     address1 =
#     return jsonify(address1)

# 如果作为主程序运行，启动应用
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)