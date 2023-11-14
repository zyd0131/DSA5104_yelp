# 导入 Flask
import json
from flask import Flask, jsonify, request
# 导入 CORS
from flask_cors import CORS
# 导入 User 类
from mysql import MySQL
from mongodb import MongoDB
from settings import *
import json
from bson import json_util
import time

# 创建一个 Flask 应用实例
app = Flask(__name__)
# 并允许来自所有域的请求
CORS(app)

app.config['JSON_AS_ASCII'] = False  # jsonify返回的中文正常显示


# 定义一个简单的路由
@app.route('/')
def test():
    # mysql = MySQL()
    # # data, time_t = mysql.mysql_5()
    # # data = mysql.mysql_7(6, 4)
    # # data = mysql.mysql_8("excellent service")
    # start_mysql = time.time()
    # data_mysql = mysql.mysql_8("excellent service")
    # end_mysql = time.time()
    # time_mysql = end_mysql - start_mysql
    # print(time_mysql)
    # return jsonify(data_mysql[:10])

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_10("Whitestown")
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)
    return json.loads(json_util.dumps(data_mogo[:10]))

    # # 创建一个字典作为模拟数据
    # data = [
    #         {"id": 1, "name": "Item 1"},
    #         {"id": 2, "name": "Item 2"},
    #         {"id": 3, "name": "Item 3"}
    #     ]
    #
    #     # {
    #     # "message": "Hello from Flask!",
    #     # "items": [
    #     #     {"id": 1, "name": "Item 1"},
    #     #     {"id": 2, "name": "Item 2"},
    #     #     {"id": 3, "name": "Item 3"}
    #     # ]
    #     # }
    # # 将字典转为 JSON 并返回
    # return jsonify(data)


@app.route('/api/num1', methods=['POST'])
def sql1():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_1()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_1()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT b.business_id, b.name,b.state " \
                   "FROM business b " \
                   "JOIN ( " \
                   "SELECT business_id, COUNT(DISTINCT user_id) AS user_count " \
                   "FROM visit " \
                   "GROUP BY business_id, user_id " \
                   "HAVING COUNT(user_id) >= 5 " \
                   ") v ON b.business_id = v.business_id " \
                   "WHERE b.state = 'NV' " \
                   "GROUP BY b.business_id, b.name " \
                   "HAVING COUNT(DISTINCT v.user_count) >= 1;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.visit.aggregate([" \
                   "{$group: {_id: { user_id: '$user_id', business_id: '$business_id'}," \
                   "visit_count: { $sum: 1 }}}," \
                   "{$match: {visit_count: { $gte: 5 }}}," \
                   "{$group: {_id: '$_id.business_id', user_count: { $sum: 1 }}}," \
                   "{$match: {user_count: { $gte: 1 }}}," \
                   "{$lookup: " \
                   "{from: 'business', localField: '_id', foreignField: '_id', " \
                   "as: 'business_info'}}," \
                   "{$unwind: '$business_info'}," \
                   "{$match: {'business_info.state': 'NV'}}" \
                   ",{$project: {_id: '$business_info._id', " \
                   "name: '$business_info.name'}}]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


@app.route('/api/num2', methods=['POST'])
def sql2():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_2()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_2()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "WITH CategoryCounts AS (" \
                   "SELECT c.category, b.business_id, b.name," \
                   "COUNT(b.business_id) OVER (PARTITION BY c.category) AS business_count" \
                   "FROM business_category c " \
                   "LEFT JOIN" \
                   "business b ON c.business_id = b.business_id) " \
                   "SELECT category, business_id, name" \
                   "FROM CategoryCounts" \
                   "WHERE business_count = (" \
                   "SELECT MAX(business_count)" \
                   "FROM CategoryCounts)" \
                   "AND business_id IN(" \
                   "SELECT business_id " \
                   "FROM business" \
                   "WHERE stars = 5);",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.aggregate([" \
                   "    {" \
                   "        $unwind: '$categories'" \
                   "    }," \
                   "    {" \
                   "        $group: {" \
                   "            _id: '$categories'," \
                   "            business_count: { $sum: 1 }," \
                   "            businesses: {" \
                   "                $push: {" \
                   "                    business_id: '$_id'," \
                   "                    name: '$name'," \
                   "                    stars: '$stars'" \
                   "                }" \
                   "            }" \
                   "        }" \
                   "    }," \
                   "    {" \
                   "        $sort: { 'business_count': -1 }" \
                   "    }," \
                   "    {" \
                   "        $limit: 1" \
                   "    }," \
                   "    {" \
                   "        $unwind: '$businesses'"
                   "    }," \
                   "    {" \
                   "        $match: {" \
                   "            'businesses.stars': 5" \
                   "        }" \
                   "    }," \
                   "    {" \
                   "        $project: {" \
                   "            _id: 0," \
                   "            category: '$_id'," \
                   "            business_id: '$businesses.business_id'," \
                   "            name: '$businesses.name'" \
                   "        }" \
                   "    }" \
                   "])", \
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num3', methods=['POST'])
def sql3():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_3()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_3()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT " \
                   "b.name, b.address as address, b.city, b.state, " \
                   "b.stars AS stars, AVG(r.stars) AS avg_stars " \
                   "FROM business b " \
                   "JOIN " \
                   "review r ON r.business_id = b.business_id " \
                   "GROUP BY " \
                   "b.business_id, b.stars,b.city " \
                   "HAVING AVG(r.stars) > b.stars " \
                   "AND b.city = 'Santa Barbara' " \
                   "AND b.stars > 3.5;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.createIndex({ stars: 1, city: 1 });" \
                   "db.review.createIndex({ business_id: 1 });" \
                   "db.business.aggregate([" \
                   "{" \
                   "$match: {" \
                   "city: 'Santa Barbara'," \
                   "stars: { $gte: 4 }," \
                   "}," \
                   "}," \
                   "{" \
                   "$lookup: {" \
                   "from: 'review'," \
                   "localField: '_id'," \
                   "foreignField: 'business_id'," \
                   "as: 'reviews'," \
                   "}," \
                   "}," \
                   "{" \
                   "$addFields: {" \
                   "avgStars: { $avg: '$reviews.stars' }," \
                   "}," \
                   "}," \
                   "{" \
                   "$match:{" \
                   "$expr:{" \
                   "$gt:['$avgStars','$stars']" \
                   "}" \
                   "}" \
                   "}," \
                   "{" \
                   "$project: {" \
                   "name: 1," \
                   "address: 1," \
                   "city: 1," \
                   "state: 1," \
                   "stars: 1," \
                   "avgStars: 1," \
                   "}," \
                   "}," \
                   "]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num4', methods=['POST'])
def sql4():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_4()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_4()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT u.user_id AS user_id, u.name AS user_name, " \
                   "COUNT(DISTINCT v1.business_id) AS johnVisitCount, " \
                   "f.friend_id, u2.name AS friend_name, " \
                   "COUNT(DISTINCT v2.business_id) AS friendVisitCount " \
                   "FROM user u " \
                   "JOIN friend f ON u.user_id = f.user_id " \
                   "JOIN user u2 ON f.friend_id = u2.user_id " \
                   "JOIN visit v1 ON v1.user_id = u.user_id " \
                   "JOIN business_category bc1 ON bc1.business_id = v1.business_id " \
                   "JOIN visit v2 ON v2.user_id = f.friend_id " \
                   "JOIN business_category bc2 ON bc2.business_id = v2.business_id " \
                   "WHERE u.name = 'John' " \
                   "AND bc1.category = 'Shopping' " \
                   "AND bc2.category = 'Shopping' " \
                   "GROUP BY u.user_id, u.name, f.friend_id, u2.name " \
                   "HAVING johnVisitCount < friendVisitCount;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.createIndex({ business_id: 1 });"
                   "db.user.createIndex({ friends: 1 });"
                   "db.visit.createIndex({ user_id: 1 });"
                   "db.visit.createIndex({ business_id: 1 });"
                   "db.user.createIndex({ name: 1 });"
                   "db.user.aggregate(["
                   "{ $match: { name: 'John' } },"
                   "{"
                   "$lookup: {"
                   "from: 'visit',"
                   "localField: '_id',"
                   "foreignField: 'user_id',"
                   "as: 'userVisit',"
                   "},"
                   "},"
                   "{"
                   "$lookup:{"
                   "from: 'business',"
                   "localField: 'userVisit.business_id',"
                   "foreignField: '_id',"
                   "as: 'businessUser',"
                   "}"
                   "},"
                   "{"
                   "$match: {"
                   "businessUser: {"
                   "$elemMatch: {"
                   "categories: 'Shopping'"
                   "}"
                   "}"
                   "}"
                   "},"
                   "{ $addFields: { userShoppingCount: "
                   "{ $size: '$businessUser' } } },"
                   "{ $unwind: '$friends' },"
                   "{"
                   "$lookup: {"
                   "from: 'visit',"
                   "localField: 'friends',"
                   "foreignField: 'user_id',"
                   "as: 'userFriendVisit',"
                   "},"
                   "},"
                   "///beginbeginbegin"
                   "{"
                   "$lookup:{"
                   "from: 'business',"
                   "localField: 'userFriendVisit.business_id',"
                   "foreignField: '_id',"
                   "as: 'businessUserFriend',"
                   "}"
                   "},"
                   "{"
                   "$match: {"
                   "businessUserFriend: {"
                   "$elemMatch: {"
                   "categories: 'Shopping'"
                   "}"
                   "}"
                   "}"
                   "},"
                   ""
                   "{ $addFields: { userFriendShoppingCount: { $size: "
                   "'$businessUserFriend' } } },"
                   "{ $lookup: { from: 'user', localField: 'friends', "
                   "foreignField: '_id', as: 'friendInfo' } },"
                   "{ $unwind: '$friendInfo' },"
                   "{"
                   "$match: {"
                   "$expr: {"
                   "$gt: ['$userFriendShoppingCount', '$userShoppingCount']"
                   "}"
                   "}"
                   "},"
                   "{"
                   "$project: {"
                   "_id: 0,"
                   "user_name: '$friendInfo.name',"
                   "yelping_since: '$yelping_since',"
                   "review_count: '$review_count'"
                   "}"
                   "}"
                   "]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


@app.route('/api/num5', methods=['POST'])
def sql5():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_5()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_5()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT user_id, (compliment_hot + compliment_more + " \
                   "compliment_profile + compliment_cute + compliment_list + " \
                   "compliment_note + compliment_plain + compliment_cool + " \
                   "compliment_funny + compliment_writer + compliment_photos + " \
                   "100 * elite_count) AS influence_score FROM compliment " \
                   "NATURAL JOIN (SELECT user_id, COUNT(elite_year) AS elite_count " \
                   "FROM user " \
                   "LEFT JOIN user_elite " \
                   "USING (user_id) " \
                   "GROUP BY user_id) AS A",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.user.aggregate(["
                   "{"
                   "$addFields: {"
                   "influence_score: {"
                   "$sum: ["
                   "'$compliment_hot',"
                   "'$compliment_more',"
                   "'$compliment_profile',"
                   "'$compliment_cute',"
                   "'$compliment_list',"
                   "'$compliment_note',"
                   "'$compliment_plain',"
                   "'$compliment_cool',"
                   "'$compliment_funny',"
                   "'$compliment_writer',"
                   "'$compliment_photos',"
                   "{$multiply: [{$size: '$elite'}, 100]}"
                   "]"
                   "}"
                   "}"
                   "},"
                   "{"
                   "$project: {"
                   "_id: 1,"
                   "influence_score: 1"
                   "}"
                   "}"
                   "])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num6', methods=['POST'])
def sql6():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_6()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_6()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT business_id, name " \
                   "FROM business " \
                   "WHERE business_id IN (SELECT A.business_id " \
                   "FROM (SELECT business_id, COUNT(date) AS cnt " \
                   "FROM checkin " \
                   "WHERE YEAR(date) = 2019 " \
                   "GROUP BY business_id) AS A " \
                   "LEFT JOIN " \
                   "(SELECT business_id, COUNT(date) AS cnt " \
                   "FROM checkin " \
                   "WHERE YEAR(date) = 2020 " \
                   "GROUP BY business_id) AS B " \
                   "ON A.business_id = B.business_id " \
                   "WHERE A.cnt IS NULL OR A.cnt < B.cnt);",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.aggregate(["
                   "{"
                   "$addFields: {"
                   "count_2019: {"
                   "$size: {"
                   "$filter: {"
                   "input: { $ifNull: ['$checkin_date', []] },"
                   "as: 'date',"
                   "cond: { $eq: [{ $year: '$$date' }, 2019] }"
                   "}"
                   "}"
                   "},"
                   "count_2020: {"
                   "$size: {"
                   "$filter: {"
                   "input: { $ifNull: ['$checkin_date', []] },"
                   "as: 'date',"
                   "cond: { $eq: [{ $year: '$$date' }, 2020] }"
                   "}"
                   "}"
                   "}"
                   "}"
                   "},"
                   "{"
                   "$match: {"
                   "$expr: {"
                   "$gt: ['$count_2020', '$count_2019']"
                   "}"
                   "}"
                   "},"
                   "{"
                   "$project: {"
                   "name: 1"
                   "}"
                   "}"
                   "])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num7', methods=['POST'])
def sql7():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_7()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_7()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "select b.business_id, b.name, avg(r.stars) as avg_rating " \
                   "from business b " \
                   "join review r " \
                   "on b.business_id = r.business_id " \
                   "where r.date >= date_sub(current_date, interval 6 month) " \
                   "and b.city = 'Santa Barbara'" \
                   "group by b.business_id, b.name " \
                   "having avg_rating > 4 " \
                   "order by avg_rating desc",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.review.aggregate("
                   "[{"
                   "$lookup: {"
                   "from: 'business',"
                   "localField: 'business_id',"
                   "foreignField: '_id',"
                   "as: 'business'"
                   "}},"
                   "{"
                   "$unwind: '$business'"
                   "},"
                   "{"
                   "$match: {"
                   "'business.city': 'Santa Barbara',"
                   "'date': { $gt: ISODate('2023-01-01T00:00:00.000Z') }"
                   "}"
                   "},"
                   "{"
                   "$group: {"
                   "_id:{business_id:'$business_id',name:'$business.name'},"
                   "avg_star: { $avg: '$stars' }"
                   "}"
                   "},"
                   "{"
                   "$match: {"
                   "'avg_star': { $gt: 4 }"
                   "}"
                   "},"
                   "{"
                   "$project: {"
                   "business_id: '$_id.business_id',"
                   "name: '$_id.name',"
                   "avg_star: '$avg_star',"
                   "_id:0"
                   "}}])"
                   "",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num8', methods=['POST'])
def sql8():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_8("excellent service")
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_8("excellent service")
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "select b.business_id, b.name, r.review_id, r.text " \
                   "from business b " \
                   "join review r " \
                   "on b.business_id = r.business_id " \
                   "where r.text like '%excellent service%';",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.review.aggregate("
                   "[{$lookup: {"
                   "from: 'business',"
                   "localField: 'business_id',"
                   "foreignField: '_id',"
                   "as: 'business'}},"
                   "{$unwind: '$business'},"
                   "{$match: {"
                   "text: { $regex: 'excellent service'}"
                   "}},",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


@app.route('/api/num9', methods=['POST'])
def sql9():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_9()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_9()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "select b.city, count(b.business_id) as num_5_star " \
                   "from business b " \
                   "where b.stars = 5 " \
                   "group by b.city " \
                   "order by num_5_star desc " \
                   "limit 3;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.aggregate([{"
                   "$match: {"
                   "'stars': 5}},"
                   "{$group: {"
                   "_id: '$city',"
                   "count: { $sum: 1 }}},"
                   "{$sort: {count: -1}},"
                   "{$limit: 3}])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)

    return data


@app.route('/api/num10', methods=['POST'])
def sql10():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_10("Whitestown")
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_10("Whitestown")
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT u.user_id, b.business_id, r.stars " \
                   "FROM business b " \
                   "JOIN review r ON b.business_id = r.business_id " \
                   "JOIN user u ON r.user_id = u.user_id " \
                   "WHERE b.city = 'Whitestown';",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.aggregate(["
                   "{$match: {"
                   "city: 'Whitestown'}},"
                   "{$lookup: {"
                   "from: 'review',"
                   "localField: '_id',"
                   "foreignField: 'business_id',"
                   "as: 'review_info'}},"
                   "{$unwind: '$review_info'},"
                   "{$lookup: {"
                   "from: 'user',"
                   "localField: 'review_info.user_id',"
                   "foreignField: '_id',"
                   "as: 'user_info'}},"
                   "{$unwind: '$user_info'},"
                   "{$project: {"
                   "_id: 0,"
                   "user_id: '$user_info._id',"
                   "business_id: '$_id',"
                   "stars: '$review_info.stars' }}])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


@app.route('/api/num11', methods=['POST'])
def sql11():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_11()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_11()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT city, category, averageStars, businessCount " \
                   "FROM ( " \
                   "SELECT b.city, c.category, " \
                   "AVG(b.stars) AS averageStars, COUNT(*) AS businessCount, " \
                   "ROW_NUMBER() OVER (PARTITION BY b.city ORDER BY COUNT(*) DESC, AVG(b.stars) DESC) AS rowNum" \
                   "FROM business as b " \
                   "JOIN business_category as c ON b.business_id = c.business_id " \
                   "WHERE b.is_open = 1 " \
                   "GROUP BY b.city, c.category ) AS ranked_categories " \
                   "WHERE businessCount > 10 and rowNum = 1;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.aggregate(["
                   "{$match: { is_open: 1 }},"
                   "{$unwind: '$categories'},"
                   "{$group: {"
                   "_id: { city: '$city', category: '$categories' },"
                   "averageStars: { $avg: '$stars' },"
                   "businessCount: { $sum: 1 }"
                   "}},"
                   "{$match: {businessCount: { $gt: 10 }}},"
                   "{$sort: { 'businessCount': -1 }},"
                   "{$group: {"
                   "_id: '$_id.city',"
                   "topCategory: { $first: '$_id.category' },"
                   "averageStars: { $first: '$averageStars' },"
                   "businessCount: { $first: '$businessCount' }"
                   "}},"
                   "{$project: {"
                   "_id: 0,"
                   "city: '$_id',"
                   "category: '$topCategory',"
                   "averageStars: 1,"
                   "businessCount: 1}}]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


@app.route('/api/num12', methods=['POST'])
def sql12():
    mysql = MySQL()
    # data, time_t = mysql.mysql_5()
    # data = mysql.mysql_7(6, 4)
    # data = mysql.mysql_8("excellent service")
    start_mysql = time.time()
    data_mysql = mysql.mysql_12()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    # print(data_mysql)
    print(time_mysql)
    # return jsonify(data)

    mongo = MongoDB()
    start_mogo = time.time()
    # data = test.get_all("DSA5104_yelp")
    # data = test.get_data("business", {"business_id": 'Pns2l4eNsfO8kk83dixA6A'}, 0)
    data_mogo = mongo.mongodb_12()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    # print(data_mogo)
    print(time_mogo)

    # construct a dict
    data = {
        "MySQL": {
            'data': data_mysql,
            'SQL': "SELECT b.name, b.address, b.city, b.postal_code, b.stars " \
                   "FROM business as b " \
                   "WHERE b.city = 'Tucson' AND b.stars > 4 AND b.is_open = 1 " \
                   "AND (6371 * acos(cos(radians(32.25346)) * cos(radians(latitude)) " \
                   "* cos(radians(longitude) - radians(-110.91179))" \
                   " + sin(radians(32.25346)) * sin(radians(latitude)))) <= 50 " \
                   "ORDER BY stars DESC;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'SQL': "db.business.find().forEach(function(doc) {"
                   "db. business.updateOne("
                   "{ _id: doc._id },"
                   "{$set: {"
                   "location: {"
                   "type: 'Point',"
                   "coordinates: [doc.longitude, doc.latitude]"
                   "}}});});"
                   "db.business.createIndex({ 'location': '2dsphere' })"
                   "db.business.find({"
                   "city: 'Tucson',"
                   "stars: { $gt: 4 },"
                   "is_open: 1,"
                   "location: {"
                   "$geoWithin: {"
                   "$centerSphere: [[-110.91179, 32.25346], 50 / 6378.1]"
                   "}}}, {"
                   "name: 1,"
                   "address: 1,"
                   "city: 1,"
                   "postal_code: 1,"
                   "stars: 1,"
                   "_id: 0"
                   "}).sort({ stars: -1 });",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    print(data)
    return data


# @app.route('/address0', methods=['GET'])
# def get_address0():
#     # 创建一个字典作为模拟数据
#     resData = {
#         "resCode": 0,  # 非0即错误 1
#         "data": [
#             {"id": 0, "text": 'a', "url": '/'},
#             {"id": 1, "text": 'b', "url": '/b'},
#             {"id": 2, "text": 'c', "url": '/c'},
#             {"id": 3, "text": 'd', "url": '/d'},
#             {"id": 4, "text": 'e', "url": '/e'},
#             {"id": 5, "text": 'f', "url": '/f'},
#             {"id": 6, "text": 'g', "url": '/g'},
#             {"id": 7, "text": 'h', "url": '/h'},
#             {"id": 8, "text": 'i', "url": '/i'},
#             {"id": 9, "text": 'j', "url": '/j'},
#         ],  # 数据位置，一般为数组
#         "message": 'Description of this request'
#     }
#     # 将字典转为 JSON 并返回
#     return jsonify(resData)


# app.route('/address1', methods=['GET'])
# def get_address1():
#     address1 =
#     return jsonify(address1)

# @app.route('/<string:address0>', methods=['POST'])
# def get_address1(address0):
#     if request.method == 'POST':
#         print("Caught a post request")
#         get_data = json.loads(request.get_data(as_text=True))
#         key = get_data['key']  # 获取key 前端js文件中
#         print("key: ", key)
#         # if book_cate in BOOK_LIST:
#         #     print(key, " is in BOOK_LIST")
#         #     print(key, secretKey)
#         #     if key == 'newest':
#         #         # select * from book_infos where book_cate='xiuzhen' order by book_last_update_time desc limit 3
#         #         print("newest")
#         #         book = Book()
#         #         sql_data = book.get_cates_newst_books_30(book_cate)
#         #         resData = {
#         #             "resCode": 0,  # 非0即错误 1
#         #             "data": sql_data,  # 数据位置，一般为数组
#         #             "message": '最新的30本图书信息查询结果'
#         #         }
#         #         return jsonify(resData)
#         #     elif key == 'most':
#         #         print("most")
#         #         book = Book()
#         #         sql_data = book.get_cates_most_books_30(book_cate)
#         #         resData = {
#         #             "resCode": 0,  # 非0即错误 1
#         #             "data": sql_data,  # 数据位置，一般为数组
#         #             "message": '最新的30本图书信息查询结果'
#         #         }
#         #         return jsonify(resData)
#         #     else:
#         #         resData = {
#         #             "resCode": 2,  # 非0即错误 1
#         #             "data": [],  # 数据位置，一般为数组
#         #             "message": '参数有误'
#         #         }
#         #         return jsonify(resData)
#     else:
#         resData = {
#             "resCode": 1,  # 非0即错误 1
#             "data": [],  # 数据位置，一般为数组
#             "message": 'Request method error'
#         }


# 如果作为主程序运行，启动应用
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
