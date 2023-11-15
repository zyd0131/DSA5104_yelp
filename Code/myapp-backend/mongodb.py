import pymongo
from datetime import datetime
import pytz
from bson import BSON
from dateutil import parser
from settings import *


class MongoDB:
    def __init__(self):
        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        self.db = self.client[MONGODB_DATABASE]

    def get_all(self, collection):
        return self.db[collection].find()

    def get_data(self, collection, condition, choice):
        ans = None
        if choice == 0:
            ans = self.db[collection].find_one(condition)
        elif choice == 1:
            save = self.db[collection].find(condition)
            ans = []
            for i in save:
                ans.append(i)
        return ans

    def mongodb_1(self):
        pipeline = [
            {
                "$group": {
                    "_id": {"user_id": "$user_id", "business_id": "$business_id"},
                    "visit_count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "visit_count": {"$gte": 5}
                }
            },
            {
                "$group": {
                    "_id": "$_id.business_id",
                    "user_count": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "user_count": {"$gte": 1}
                }
            },
            {
                "$lookup": {
                    "from": "business",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "business_info"
                }
            },
            {
                "$unwind": "$business_info"
            },
            {
                "$match": {
                    "business_info.state": "NV"
                }
            },
            {
                "$project": {
                    "_id": "$business_info._id",
                    "name": "$business_info.name"
                }
            }
        ]
        result = self.db["visit"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_2(self):
        pipeline = [
            {
                "$unwind": "$categories"
            },
            {
                "$group": {
                    "_id": "$categories",
                    "business_count": {"$sum": 1},
                    "businesses": {
                        "$push": {
                            "business_id": "$_id",
                            "name": "$name",
                            "stars": "$stars"
                        }
                    }
                }
            },
            {
                "$sort": {"business_count": -1}
            },
            {
                "$limit": 1
            },
            {
                "$unwind": "$businesses"
            },
            {
                "$match": {
                    "businesses.stars": 5
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id",
                    "business_id": "$businesses.business_id",
                    "name": "$businesses.name"
                }
            }
        ]
        result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_3(self):
        coll = self.db["business"]
        # coll.create_index({'stars': 1, 'city': 1})
        # coll.create_index('stars')
        # coll.create_index('city')
        # self.db["review"].create_index('business_id')
        pipeline = [
            {
                "$match": {
                    "city": 'Santa Barbara',
                    "stars": {"$gt": 4},
                },
            },
            {
                "$lookup": {
                    "from": 'review',
                    "localField": '_id',
                    "foreignField": 'business_id',
                    "as": 'reviews',
                },
            },
            {"$addFields": {
                "filteredReviews": {
                    "$filter": {
                        "input": '$reviews',
                        "as": 'review',
                        "cond": {
                            "$gt": ['$$review.date', datetime(2020, 1, 1, 0, 0, 0)],
                        }, }, }, }, },
            {"$addFields": {
                "avgBusinessStars": {
                    "$avg": '$filteredReviews.stars',
                }, } },
            {"$match": {
                "$expr": {
                    "$gt": ["$avgBusinessStars", "$stars"], }, }, },
            {"$project": {
                "name": 1,
                "address": 1,
                "city": 1,
                "state": 1,
                "stars": 1,
                "avgBusinessStars": 1,
                # "filteredReviews": 1,
                "_id": 0
            }, }
        ]
        result = coll.aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_4(self):
        coll = self.db["user"]
        # self.db["business"].create_index('business_id')
        # coll.create_index('friends')
        # self.db["visit"].create_index('user_id')
        # self.db["visit"].create_index('business_id')
        # coll.create_index('name')

        pipeline = [
            {"$match": {"name": "John"}},
            {
                "$lookup": {
                    "from": "visit",
                    "localField": "_id",
                    "foreignField": "user_id",
                    "as": "userVisit",
                },
            },
            {
                "$lookup": {
                    "from": "business",
                    "localField": "userVisit.business_id",
                    "foreignField": "_id",
                    "as": "businessUser",
                }
            },
            {
                "$match": {
                    "businessUser": {
                        "$elemMatch": {
                            "categories": "Shopping"
                        }
                    }
                }
            },

            {"$addFields": {"userShoppingCount": {"$size": "$businessUser"}}},
            {"$unwind": "$friends"},
            {
                "$lookup": {
                    "from": "visit",
                    "localField": "friends",
                    "foreignField": "user_id",
                    "as": "userFriendVisit",
                },
            },
            {
                "$lookup": {
                    "from": "business",
                    "localField": "userFriendVisit.business_id",
                    "foreignField": "_id",
                    "as": "businessUserFriend",
                }
            },

            {
                "$match": {
                    "businessUserFriend": {
                        "$elemMatch": {
                            "categories": "Shopping"
                        }
                    }
                }
            },

            {"$addFields": {"userFriendShoppingCount": {"$size": "$businessUserFriend"}}},
            {"$lookup": {
                "from": "user", "localField": "friends", "foreignField": "_id", "as": "friendInfo"}},
            {"$unwind": "$friendInfo"},
            {
                "$match": {
                    "$expr": {
                        "$gt": ["$userFriendShoppingCount", "$userShoppingCount"]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "user_name": "$friendInfo.name",
                    # "yelping_since": "$yelping_since",
                    "review_count": "$review_count"
                }
            },
            {"$limit": 4650}
        ]
        result = coll.aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_5(self):
        pipeline = [
            {
                "$addFields": {
                    "influence_score": {
                        "$sum": [
                            "$compliment_hot",
                            "$compliment_more",
                            "$compliment_profile",
                            "$compliment_cute",
                            "$compliment_list",
                            "$compliment_note",
                            "$compliment_plain",
                            "$compliment_cool",
                            "$compliment_funny",
                            "$compliment_writer",
                            "$compliment_photos",
                            {"$multiply": [{"$size": "$elite"}, 100]}
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "influence_score": 1}
            },
            {
                "$sort": {
                    "influence_score": -1
                }
            },
            {
                "$limit": 20
            }
        ]
        result = self.db["user"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_6(self):
        pipeline = [
            {
                "$addFields": {
                    "count_2019": {
                        "$size": {
                            "$filter": {
                                "input": {"$ifNull": ["$checkin_date", []]},
                                "as": "date",
                                "cond": {"$eq": [{"$year": "$$date"}, 2019]}
                            }
                        }
                    },
                    "count_2020": {
                        "$size": {
                            "$filter": {
                                "input": {"$ifNull": ["$checkin_date", []]},
                                "as": "date",
                                "cond": {"$eq": [{"$year": "$$date"}, 2020]}
                            }
                        }
                    }
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$gt": ["$count_2020", "$count_2019"]
                    }
                }
            },
            {
                "$project": {
                    "name": 1
                }
            }
        ]
        result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_7(self):
        pipeline = [
            {
                "$lookup": {
                    "from": "business",
                    "localField": "business_id",
                    "foreignField": "_id",
                    "as": "business"
                }
            },
            {
                "$unwind": "$business"
            },
            {
                "$match": {
                    "business.city": "Santa Barbara",
                    "date": {"$gte": datetime(2023, 1, 1, 0, 0, 0)}
                }
            },
            {
                "$group": {
                    "_id": {"business_id": "$business_id", "name": "$business.name"},
                    "avg_star": {"$avg": "$stars"}
                }
            },
            {
                "$match": {
                    "avg_star": {"$gt": 4}
                }
            },
            {"$sort": {"avg_star": -1}},
            {
                "$project": {
                    "business_id": "$_id.business_id",
                    "name": "$_id.name",
                    "avg_star": "$avg_star",
                    "_id": 0
                }
            }
        ]
        result = self.db["review"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_8(self, str):
        pipeline = [
            {
                "$lookup": {
                    "from": "business",
                    "localField": "business_id",
                    "foreignField": "_id",
                    "as": "business"
                }
            },
            {
                "$unwind": "$business"
            },
            {
                "$match": {
                    "text": {"$regex": str}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "business_id": '$business_id',
                    "name": "$business.name",
                    "text": '$text'
                }
            }
        ]
        result = self.db["review"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_9(self):
        pipeline = [
            {
                "$match": {
                    "stars": 5
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            },
            {
                "$limit": 1
            }
        ]
        result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
        return ans

    def mongodb_10(self, str):
        pipeline = [
            {
                "$match": {
                    "city": str
                }
            },
            {
                "$lookup": {
                    "from": "review",
                    "localField": "_id",
                    "foreignField": "business_id",
                    "as": "review_info"
                }
            },
            {
                "$unwind": "$review_info"
            },
            {
                "$lookup": {
                    "from": "user",
                    "localField": "review_info.user_id",
                    "foreignField": "_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$project": {
                    "_id": 0,
                    "user_id": "$user_info._id",
                    "business_id": "$_id",
                    "stars": "$review_info.stars"
                }
            }
            # {"limit": 1648}
        ]
        result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_11(self):
        pipeline = [
            {
                "$match": {"is_open": 1}
            },
            {
                "$unwind": "$categories"
            },
            {
                "$group": {
                    "_id": {"city": "$city", "category": "$categories"},
                    "averageStars": {"$avg": "$stars"},
                    "businessCount": {"$sum": 1}
                }
            },
            {
                "$match": {
                    "businessCount": {"$gt": 10}
                }
            },
            {
                "$sort": {"businessCount": -1}
            },
            {
                "$group": {
                    "_id": "$_id.city",
                    "topCategory": {"$first": "$_id.category"},
                    "averageStars": {"$first": "$averageStars"},
                    "businessCount": {"$first": "$businessCount"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "category": "$topCategory",
                    "averageStars": 1,
                    "businessCount": 1
                }
            }
        ]
        result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

    def mongodb_12(self):
        coll = self.db["business"]
        # for doc in coll.find():
        #     coll.update_one({"_id": doc["_id"]},
        #                     {"$set": {
        #                         "location": {
        #                             "type": "Point",
        #                             "coordinates": [doc["longitude"], doc["latitude"]]}}})
        # coll.create_index({"location": "2dsphere"})
        result = coll.find({
            "city": "Tucson",
            "stars": {"$gt": 4},
            "is_open": 1,
            "location": {
                "$geoWithin": {
                    "$centerSphere": [[-110.91179, 32.25346], 50 / 6378.1]
                }}}, {
            "name": 1,
            "address": 1,
            "city": 1,
            "postal_code": 1,
            "stars": 1,
            "_id": 0
        }).sort({"stars": -1})
        # result = self.db["business"].aggregate(pipeline)
        ans = []
        for i in result:
            ans.append(i)
            # print(i)
        return ans

        # def delete(self, collection, id):
        #     return self.db[collection].delete_one({"id": id})
        #
        # def delete_all(self, collection):
        #     return self.db[collection].delete_many({})
        #
        # def delete_data(self, collection, condition, choice):
        #     ans = None
        #     if choice == 0:
        #         ans = self.db[collection].find_one_and_delete(condition)
        #     elif choice == 1:
        #         ans = self.db[collection].delete_many(condition)
        #     print("ans_count: ", ans.deleted_count)
        #     if ans.deleted_count == 0:
        #         print("delete failed")
        #         return False
        #     else:
        #         print("delete success")
        #         return True
        #
        # def update(self, collection, id, business):
        #     return self.db[collection].update_one({"id": id}, {"$set": business})
        #
        # def update_data(self, collection, find_condition, update_condition, choice):
        #     ans = None
        #     if choice == 0:
        #         ans = self.db[collection].find_one_and_update(find_condition, {"$set": update_condition})
        #     elif choice == 1:
        #         ans = self.db[collection].update_many(find_condition, {"$set": update_condition})
        #     print("ans_count: ", ans.modified_count)
        #     if ans.modified_count == 0:
        #         print("update failed")
        #         return False
        #     else:
        #         print("update success")
        #         return True
        #
        # def add(self, collection, business):
        #     return self.db[collection].insert_one(business)
        #
        # def add_data(self, collection, data):
        #     if isinstance(data, list):
        #         ans = self.db[collection].insert_many(data)
        #         if ans:
        #             return {"status": 1, "msg": "add list success"}
        #     elif isinstance(data, dict):
        #         ans = self.db[collection].insert_one(data)
        #         if ans:
        #             return {"status": 1, "msg": "add dict success"}
        #     else:
        #         return {"status": 0, "msg": "add failed"}
