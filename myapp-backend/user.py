from pymysql import connect
from pymysql.cursors import DictCursor # 为了返回字典形式
from settings import *

class User(object):
    def __init__(self):  # 创建对象同时要执行的代码
        self.conn = connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8'
        )
        # self.cursor = self.conn.cursor()
        self.cursor = self.conn.cursor(DictCursor) # 这个可以让他返回字典的形式

    def __del__(self): # 释放对象同时要执行的代码
        self.cursor.close()
        self.conn.close()

    def get_all_user(self):
        sql = 'select * from user'
        self.cursor.execute(sql)
        data = []
        for temp in self.cursor.fetchall():
            print(temp)
            data.append(temp)
        return data

    def get_user_by_id(self, user_id):
        sql = "select * from user where user_id ='{}'".format(user_id)
        self.cursor.execute(sql)
        data = []
        for temp in self.cursor.fetchall():
            print(temp)
            data.append(temp)
        return data

