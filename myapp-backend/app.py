# 导入 Flask
from flask import Flask, jsonify
# 导入 CORS
from flask_cors import CORS
# 导入 User 类
from user import User
from settings import *

# 创建一个 Flask 应用实例
app = Flask(__name__)
# 并允许来自所有域的请求
CORS(app)

# 定义一个简单的路由
@app.route('/')
def get_user():
    user = User()
    data = user.get_all_user()
    return data
    # return jsonify(data)

@app.route('/api/data', methods=['GET'])
def get_data():
    # 创建一个字典作为模拟数据
    data = {
        "message": "Hello from Flask!",
        "items": [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
            {"id": 3, "name": "Item 3"}
        ]
    }
    # 将字典转为 JSON 并返回
    return jsonify(data)


# 如果作为主程序运行，启动应用
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)