import pymongo

#mongodb初始化
def initMongoDB(database, collection):
    client = pymongo.MongoClient(host= '127.0.0.1', port=27017)
    db = client[database]
    # coll = db.get_collection(collection)
    return db

class MongoDB:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["test"]
        # self.collection = self.db["test0"]

    # def get(self, collection, id):
    #     return self.db[collection].find_one({"_id": id})

    def get_all(self, collection):
        return self.db[collection].find()

    def get_data(self, collection, condition, choice):
        ans = None
        if choice == 0:
            ans = self.db[collection].find_one(condition)
            print(ans)
        elif choice == 1:
            save = self.db[collection].find(condition)
            ans = []
            for i in save:
                ans.append(i)
            print(ans)
        return ans

    def delete(self, collection, id):
        return self.db[collection].delete_one({"id": id})

    def delete_all(self, collection):
        return self.db[collection].delete_many({})

    def delete_data(self, collection, condition, choice):
        ans = None
        if choice == 0:
            ans = self.db[collection].find_one_and_delete(condition)
        elif choice == 1:
            ans = self.db[collection].delete_many(condition)
        print("ans_count: ", ans.deleted_count)
        if ans.deleted_count == 0:
            print("delete failed")
            return False
        else:
            print("delete success")
            return True

    def update(self, collection, id, business):
        return self.db[collection].update_one({"id": id}, {"$set": business})

    def update_data(self, collection, find_condition, update_condition, choice):
        ans = None
        if choice == 0:
            ans = self.db[collection].find_one_and_update(find_condition, {"$set": update_condition})
        elif choice == 1:
            ans = self.db[collection].update_many(find_condition, {"$set": update_condition})
        print("ans_count: ", ans.modified_count)
        if ans.modified_count == 0:
            print("update failed")
            return False
        else:
            print("update success")
            return True

    def add(self, collection, business):
        return self.db[collection].insert_one(business)

    def add_data(self, collection, data):
        if isinstance(data, list):
            ans = self.db[collection].insert_many(data)
            if ans:
                return {"status": 1, "msg": "add list success"}
        elif isinstance(data, dict):
            ans = self.db[collection].insert_one(data)
            if ans:
                return {"status": 1, "msg": "add dict success"}
        else:
            return {"status": 0, "msg": "add failed"}

