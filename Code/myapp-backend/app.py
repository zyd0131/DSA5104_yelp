import json
from flask import Flask, jsonify, request
from flask_cors import CORS
from mysql import MySQL
from mongodb import MongoDB
from settings import *
import json
from bson import json_util
from logzero import logger
import time

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app)

"""
如果改变路由，该顺序需要也改动一下这个
"""
query_List = [
    'Find the name of merchants in NV state with loyal customers (visited by at least 1 customer 5 times or more)',
    'Find out the category of merchants with the largest number as well as all the merchant with 5 stars under this category.',
    'Find out which stores in city: Santa Barbara with a star rating greater than 3.5 have an average star rating greater than the merchant\'s star rating in reviews after January 1, 2020 at 0:00 pm.',
    'Find out who John\'s friends are. Find the people who go shopping more often than him.',
    'The "Influence Score" is generated from the sum of the 11 attributes of the user\'s COMPLIMENT plus 100 times the user\'s ELITE number of years, listing the 20 users with the highest scores.',
    'Find all the businesses that had more check-in numbers in 2020 than in 2019.',
    'Select those merchants with an average rating higher than 4 star from 2023-01-01 to now in \'Santa Barbara\'',
    'Search review data to find all reviews that mention "excellent service" and display essential details about the merchant.',
    'Analyze which city has the most 5-star merchants and return the number.',
    'Providing User-Item Matrix for Collaborative Filtering Algorithms.',
    'Count the average star ratings for each category and the number of merchants (open doors) in each category in each city and select the category with the highest number of merchants and average star ratings in the city.',
    'Use MongoDB\'s geospatial indexing capabilities to find the highest-rated merchants within 5 kilometers of Tucson\'s downtown location. (Latitude: 32.25346° N Longitude: 110.91179° W)']


@app.route('/', methods=['GET'])
def test():
    mysql = MySQL()
    start_mysql = time.time()
    data_mysql = mysql.mysql_1()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    print(time_mysql)
    return data_mysql[:10]

@app.route('/api/num1', methods=['GET'])
def sql1():
    mysql = MySQL()
    start_mysql = time.time()
    data_mysql = mysql.mysql_1()
    end_mysql = time.time()
    time_mysql = end_mysql - start_mysql
    print("======================================")
    print(time_mysql)

    mongo = MongoDB()
    start_mogo = time.time()
    data_mogo = mongo.mongodb_1()
    end_mogo = time.time()
    time_mogo = end_mogo - start_mogo
    print("======================================")
    print(time_mogo)

    # construct a dict
    data = {
        'query': query_List[0],
        "MySQL": {
            'data': data_mysql[:50],
            'time': time_mysql,
            'tps': len(data_mysql),
            'SQL': "SELECT b.business_id, b.name " \
                   "FROM business b " \
                   "JOIN ( " \
                   "SELECT business_id, COUNT(DISTINCT user_id) AS user_count " \
                   "FROM visit " \
                   "GROUP BY business_id, user_id " \
                   "HAVING COUNT(user_id) >= 5 " \
                   ") v ON b.business_id = v.business_id " \
                   "WHERE b.state = 'NV' " \
                   "GROUP BY b.business_id, b.name " \
                   "HAVING COUNT(DISTINCT v.user_count) >= 1;"
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[:50])),
            'time': time_mogo,
            'tps': len(data_mogo),
            'MQL': "db.visit.aggregate([" \
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
                   "name: '$business_info.name'}}]);"
        }
    }
    return jsonify(data)


@app.route('/api/num2', methods=['GET'])
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
        'query': query_List[1],
        "MySQL": {
            'data': data_mysql[:50],
            'SQL': "WITH CategoryCounts AS (" \
                   "SELECT c.category, b.business_id, b.name," \
                   "COUNT(b.business_id) OVER (PARTITION BY c.category) AS business_count " \
                   "FROM business_category c " \
                   "LEFT JOIN " \
                   "business b ON c.business_id = b.business_id) " \
                   "SELECT category, business_id, name " \
                   "FROM CategoryCounts " \
                   "WHERE business_count = (" \
                   "SELECT MAX(business_count)" \
                   "FROM CategoryCounts)" \
                   "AND business_id IN(" \
                   "SELECT business_id " \
                   "FROM business " \
                   "WHERE stars = 5);",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[:50])),
            'MQL': "db.business.aggregate([" \
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
    # print(data)
    return jsonify(data)


@app.route('/api/num3', methods=['GET'])
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
        'query': query_List[2],
        "MySQL": {
            'data': data_mysql[:50],
            'SQL': "SELECT " \
                   "b.name, b.address as address, b.city, b.state, " \
                   "b.stars AS stars, AVG(r.stars) AS avg_stars " \
                   "FROM business b " \
                   "JOIN " \
                   "review r ON r.business_id = b.business_id " \
                   "WHERE r.date > '2020-01-01T00:00:00' " \
                   "GROUP BY " \
                   "b.business_id, b.stars,b.city " \
                   "HAVING AVG(r.stars) > b.stars " \
                   "AND b.city = 'Santa Barbara' " \
                   "AND b.stars > 4;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[:50])),
            'MQL': "db.business.createIndex({ stars: 1, city: 1 });" \
                   "db.review.createIndex({ business_id: 1 });" \
                   "db.business.aggregate([" \
                   "{" \
                   "$match: {" \
                   "city: 'Santa Barbara'," \
                   "stars: { $gt: 4 }," \
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
                   "filteredReviews: {" \
                   "$filter: {" \
                   "input: '$reviews'," \
                   "as: 'review'," \
                   "cond: {" \
                   "$gt: ['$$review.date', new Date('2020-01-01T00:00:00')]," \
                   "},},},},}," \
                   "{$addFields:{" \
                   "avgBusinessStars: {" \
                   "$avg: '$filteredReviews.stars'," \
                   "},}}," \
                   "{$match: {" \
                   "$expr: {" \
                   "$gt: ['$avgBusinessStars', '$stars'],},},}," \
                   "{" \
                   "$project: {" \
                   "name: 1," \
                   "address: 1," \
                   "city: 1," \
                   "state: 1," \
                   "stars: 1," \
                   "avgStars: 1," \
                   "_id: 0," \
                   "}," \
                   "}," \
                   "]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num4', methods=['GET'])
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
        'query': query_List[3],
        "MySQL": {
            'data': data_mysql[:50],
            'SQL': "select A.friend_name, A.friendVisitCount from (" \
              "SELECT u.user_id AS user_id, u.name AS user_name, " \
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
              "HAVING johnVisitCount < friendVisitCount) as A;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[:50])),
            'MQL': "db.business.createIndex({ business_id: 1 });" \
                   "db.user.createIndex({ friends: 1 });" \
                   "db.visit.createIndex({ user_id: 1 });" \
                   "db.visit.createIndex({ business_id: 1 });" \
                   "db.user.createIndex({ name: 1 });" \
                   "db.user.aggregate([" \
                   "{ $match: { name: 'John' } }," \
                   "{" \
                   "$lookup: {" \
                   "from: 'visit'," \
                   "localField: '_id'," \
                   "foreignField: 'user_id'," \
                   "as: 'userVisit'," \
                   "}," \
                   "}," \
                   "{" \
                   "$lookup:{" \
                   "from: 'business'," \
                   "localField: 'userVisit.business_id'," \
                   "foreignField: '_id'," \
                   "as: 'businessUser'," \
                   "}}," \
                   "{$match: {" \
                   "businessUser: {" \
                   "$elemMatch: {" \
                   "categories: 'Shopping'" \
                   "}" \
                   "}" \
                   "}" \
                   "}," \
                   "{ $addFields: { userShoppingCount: " \
                   "{ $size: '$businessUser' } } }," \
                   "{ $unwind: '$friends' }," \
                   "{" \
                   "$lookup: {" \
                   "from: 'visit'," \
                   "localField: 'friends'," \
                   "foreignField: 'user_id'," \
                   "as: 'userFriendVisit'," \
                   "}," \
                   "}," \
                   "{" \
                   "$lookup:{" \
                   "from: 'business'," \
                   "localField: 'userFriendVisit.business_id'," \
                   "foreignField: '_id'," \
                   "as: 'businessUserFriend'," \
                   "}" \
                   "}," \
                   "{" \
                   "$match: {" \
                   "businessUserFriend: {" \
                   "$elemMatch: {" \
                   "categories: 'Shopping'" \
                   "}" \
                   "}" \
                   "}" \
                   "}," \
                   "{ $addFields: { userFriendShoppingCount: { $size: " \
                   "'$businessUserFriend' } } }," \
                   "{ $lookup: { from: 'user', localField: 'friends', " \
                   "foreignField: '_id', as: 'friendInfo' } }," \
                   "{ $unwind: '$friendInfo' }," \
                   "{" \
                   "$match: {" \
                   "$expr: {" \
                   "$gt: ['$userFriendShoppingCount', '$userShoppingCount']" \
                   "}" \
                   "}" \
                   "}," \
                   "{" \
                   "$project: {" \
                   "_id: 0," \
                   "user_name: '$friendInfo.name'," \
                   "review_count: '$review_count'" \
                   "}" \
                   "}" \
                   "]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num5', methods=['GET'])
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
        'query': query_List[4],
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
                   "GROUP BY user_id) AS A " \
                   "ORDER BY influence_score DESC LIMIT 20;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'MQL': "db.user.aggregate([" \
                   "{" \
                   "$addFields: {" \
                   "influence_score: {" \
                   "$sum: [" \
                   "'$compliment_hot'," \
                   "'$compliment_more'," \
                   "'$compliment_profile'," \
                   "'$compliment_cute'," \
                   "'$compliment_list'," \
                   "'$compliment_note'," \
                   "'$compliment_plain'," \
                   "'$compliment_cool'," \
                   "'$compliment_funny'," \
                   "'$compliment_writer'," \
                   "'$compliment_photos'," \
                   "{$multiply: [{$size: '$elite'}, 100]}" \
                   "]" \
                   "}" \
                   "}" \
                   "}," \
                   "{" \
                   "$project: {" \
                   "_id: 1," \
                   "influence_score: 1" \
                   "}" \
                   "}," \
                   "{$sort: {influence_score: -1}}," \
                   "{$limit: 20}" \
                   "])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)

    return jsonify(data)


@app.route('/api/num6', methods=['GET'])
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
        'query': query_List[5],
        "MySQL": {
            'data': data_mysql[0:50],
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
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.business.aggregate(["\
                   "{"\
                   "$addFields: {"\
                   "count_2019: {"\
                   "$size: {"\
                   "$filter: {"\
                   "input: { $ifNull: ['$checkin_date', []] },"\
                   "as: 'date',"\
                   "cond: { $eq: [{ $year: '$$date' }, 2019] }"\
                   "}"\
                   "}"\
                   "},"\
                   "count_2020: {"\
                   "$size: {"\
                   "$filter: {"\
                   "input: { $ifNull: ['$checkin_date', []] },"\
                   "as: 'date',"\
                   "cond: { $eq: [{ $year: '$$date' }, 2020] }"\
                   "}"\
                   "}"\
                   "}"\
                   "}"\
                   "},"\
                   "{"\
                   "$match: {"\
                   "$expr: {"\
                   "$gt: ['$count_2020', '$count_2019']"\
                   "}"\
                   "}"\
                   "},"\
                   "{"\
                   "$project: {"\
                   "name: 1"\
                   "}"\
                   "}"\
                   "])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num7', methods=['GET'])
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
        'query': query_List[6],
        "MySQL": {
            'data': data_mysql[0:50],
            'SQL': "select b.business_id, b.name, avg(r.stars) as avg_rating " \
                   "from business b " \
                   "join review r " \
                   "on b.business_id = r.business_id " \
                   "where r.date > '2023-01-01 00:00:00' " \
                   "and b.city = 'Santa Barbara'" \
                   "group by b.business_id, b.name " \
                   "having avg_rating > 4 " \
                   "order by avg_rating desc",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.review.aggregate("\
                   "[{"\
                   "$lookup: {"\
                   "from: 'business',"\
                   "localField: 'business_id',"\
                   "foreignField: '_id',"\
                   "as: 'business'"\
                   "}},"\
                   "{"\
                   "$unwind: '$business'"\
                   "},"\
                   "{"\
                   "$match: {"\
                   "'business.city': 'Santa Barbara',"\
                   "'date': { $gt: ISODate('2023-01-01T00:00:00.000Z') }"\
                   "}"\
                   "},"\
                   "{"\
                   "$group: {"\
                   "_id:{business_id:'$business_id',name:'$business.name'},"\
                   "avg_star: { $avg: '$stars' }"\
                   "}"\
                   "},"\
                   "{"\
                   "$match: {"\
                   "'avg_star': { $gt: 4 }"\
                   "}"\
                   "},"\
                   "{'$sort': {'avg_star': -1}},"
                   "{"\
                   "$project: {"\
                   "business_id: '$_id.business_id',"\
                   "name: '$_id.name',"\
                   "avg_star: '$avg_star',"\
                   "_id:0"\
                   "}}])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)

    return jsonify(data)


@app.route('/api/num8', methods=['GET'])
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
        'query': query_List[7],
        "MySQL": {
            'data': data_mysql[0:50],
            'SQL': "select b.business_id, b.name, r.review_id, r.text " \
                   "from business b " \
                   "join review r " \
                   "on b.business_id = r.business_id " \
                   "where r.text like '%excellent service%';",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.review.aggregate("\
                   "[{$lookup: {"\
                   "from: 'business',"\
                   "localField: 'business_id',"\
                   "foreignField: '_id',"\
                   "as: 'business'}},"\
                   "{$unwind: '$business'},"\
                   "{$match: {"\
                   "text: { $regex: 'excellent service'}"\
                   "}},",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num9', methods=['GET'])
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
        'query': query_List[8],
        "MySQL": {
            'data': data_mysql,
            'SQL': "select b.city, count(b.business_id) as num_5_star " \
                   "from business b " \
                   "where b.stars = 5 " \
                   "group by b.city " \
                   "order by num_5_star desc " \
                   "limit 1;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo)),
            'MQL': "db.business.aggregate([{"\
                   "$match: {"\
                   "'stars': 5}},"\
                   "{$group: {"\
                   "_id: '$city',"\
                   "count: { $sum: 1 }}},"\
                   "{$sort: {count: -1}},"\
                   "{$limit: 1}])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)

    return jsonify(data)


@app.route('/api/num10', methods=['GET'])
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
        'query': query_List[9],
        "MySQL": {
            'data': data_mysql[0:50],
            'SQL': "SELECT u.user_id, b.business_id, r.stars " \
                   "FROM business b " \
                   "JOIN review r ON b.business_id = r.business_id " \
                   "JOIN user u ON r.user_id = u.user_id " \
                   "WHERE b.city = 'Whitestown';",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.business.aggregate(["\
                   "{$match: {"\
                   "city: 'Whitestown'}},"\
                   "{$lookup: {"\
                   "from: 'review',"\
                   "localField: '_id',"\
                   "foreignField: 'business_id',"\
                   "as: 'review_info'}},"\
                   "{$unwind: '$review_info'},"\
                   "{$lookup: {"\
                   "from: 'user',"\
                   "localField: 'review_info.user_id',"\
                   "foreignField: '_id',"\
                   "as: 'user_info'}},"\
                   "{$unwind: '$user_info'},"\
                   "{$project: {"\
                   "_id: 0,"\
                   "user_id: '$user_info._id',"\
                   "business_id: '$_id',"\
                   "stars: '$review_info.stars' }}])",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num11', methods=['GET'])
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
        'query': query_List[10],
        "MySQL": {
            'data': data_mysql[0:50],
            'SQL': "SELECT city, category, averageStars, businessCount " \
                   "FROM ( " \
                   "SELECT b.city, c.category, " \
                   "AVG(b.stars) AS averageStars, COUNT(*) AS businessCount, " \
                   "ROW_NUMBER() OVER (PARTITION BY b.city ORDER BY COUNT(*) DESC, AVG(b.stars) DESC) AS rowNum " \
                   "FROM business as b " \
                   "JOIN business_category as c ON b.business_id = c.business_id " \
                   "WHERE b.is_open = 1 " \
                   "GROUP BY b.city, c.category ) AS ranked_categories " \
                   "WHERE businessCount > 10 and rowNum = 1;",
            'time': time_mysql,
            'tps': len(data_mysql)
        },
        "MongoDB": {
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.business.aggregate(["\
                   "{$match: { is_open: 1 }},"\
                   "{$unwind: '$categories'},"\
                   "{$group: {"\
                   "_id: { city: '$city', category: '$categories' },"\
                   "averageStars: { $avg: '$stars' },"\
                   "businessCount: { $sum: 1 }"\
                   "}},"\
                   "{$match: {businessCount: { $gt: 10 }}},"\
                   "{$sort: { 'businessCount': -1 }},"\
                   "{$group: {"\
                   "_id: '$_id.city',"\
                   "topCategory: { $first: '$_id.category' },"\
                   "averageStars: { $first: '$averageStars' },"\
                   "businessCount: { $first: '$businessCount' }"\
                   "}},"\
                   "{$project: {"\
                   "_id: 0,"\
                   "city: '$_id',"\
                   "category: '$topCategory',"\
                   "averageStars: 1,"\
                   "businessCount: 1}}]);",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


@app.route('/api/num12', methods=['GET'])
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
        'query': query_List[11],
        "MySQL": {
            'data': data_mysql[0:50],
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
            'data': json.loads(json_util.dumps(data_mogo[0:50])),
            'MQL': "db.business.find().forEach(function(doc) {"\
                   "db. business.updateOne("\
                   "{ _id: doc._id },"\
                   "{$set: {"\
                   "location: {"\
                   "type: 'Point',"\
                   "coordinates: [doc.longitude, doc.latitude]"\
                   "}}});});"\
                   "db.business.createIndex({ 'location': '2dsphere' })"\
                   "db.business.find({"\
                   "city: 'Tucson',"\
                   "stars: { $gt: 4 },"\
                   "is_open: 1,"\
                   "location: {"\
                   "$geoWithin: {"\
                   "$centerSphere: [[-110.91179, 32.25346], 50 / 6378.1]"\
                   "}}}, {"\
                   "name: 1,"\
                   "address: 1,"\
                   "city: 1,"\
                   "postal_code: 1,"\
                   "stars: 1,"\
                   "_id: 0"\
                   "}).sort({ stars: -1 });",
            'time': time_mogo,
            'tps': len(data_mogo)
        }
    }
    # print(data)
    return jsonify(data)


# 如果作为主程序运行，启动应用
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)
