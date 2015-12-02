# _*_ coding: utf8 _*_

# 注册用户
# 正确性 80%
# TODO: 找真实数据测试 pc 以及mobile

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://10.8.8.111:27017')
db = dbClient['yangcong-prod25']

# open collections
points = db['points']
users = db['users']
userAttr = db['userAttr']

# configure daterange
startDate = datetime.datetime(2015,7,13)
endDate   = startDate + datetime.timedelta(days = 2)

lastWeekStartDate = datetime.datetime(2015,7,15)
lastWeekEndDate   = lastWeekStartDate + datetime.timedelta(days = 2)

def regUsers(startDate, endDate, fromEnd):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": {"$in": fromEnd}
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

mobile = ["android", "ios"]
mobileRegUsers = regUsers(startDate, endDate, mobile)
print("mobile register users:")
print(len(mobileRegUsers))

pcRegUsers = regUsers(startDate, endDate, ["pc"])
print("pc register users:")
print(len(pcRegUsers))
