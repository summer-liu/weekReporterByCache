# _*_ coding: utf8 _*_

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
startDate = datetime.datetime(2015,7,16)
endDate   = startDate + datetime.timedelta(days = 2)

lastWeekStartDate = datetime.datetime(2015,7,15)
lastWeekEndDate   = lastWeekStartDate + datetime.timedelta(days = 2)

# ###### Part.3 周活跃 ######
# PC周活跃统计
# Mobile 周活跃总计
# 两端同活跃统计
# ###### Part.3 周活跃 ######
#
# ###### Part.5 周留存 ######
# PC 上周新增(周内注册并激活)用户：
# Mobile 上周新增用户
# PC 本周留存:
# Mobile 本周留存:
# PC 本周新增：
# Mobile 本周新增:
# ###### Part.5 周留存 ######

# 新增用户
# 筛选注册并且指定首次激活时间范围内的用户
def newUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "activatedTime": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered" : True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    return list(userAttr.aggregate(pipeLine))[0]['users']

newUsersList = newUsers(startDate, endDate)
print("This week new users: %s")%len(newUsersList)

# "os" : {
# 		"pc" : true,
# 		"android" : false,
# 		"ios" : false
# }

def pcNewUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "activatedTime": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered" : True,
            "os.pc": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    return list(userAttr.aggregate(pipeLine))[0]['users']

pcnewUsersList = pcNewUsers(startDate, endDate)
print("This week pc new users: %s")%len(pcnewUsersList)

# 活跃用户
# 筛选注册并且近期某一时间范围内活跃的用户
def activatedUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    return list(userAttr.aggregate(pipeLine))[0]['users']

activatedUsersList = activatedUsers(startDate, endDate)
print("This week activated users: %s")%len(activatedUsersList)

# 上周新增，留存至本周还活跃的用户
def lastWeekRetention(lastWeekStartDate, lastWeekEndDate, thisWeekStartDate, thisWeekEndDate):
    laskWeekUsersList = newUsers(lastWeekStartDate, lastWeekEndDate)
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": thisWeekStartDate,
                "$lt": thisWeekEndDate
            },
            "userId": {"$in": laskWeekUsersList}
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]

    return list(userAttr.aggregate(pipeLine))[0]['users']

lastWeekRetentionList = (lastWeekStartDate, lastWeekEndDate, startDate, endDate)
print("lastWeekRetentionList: %s")%len(lastWeekRetentionList)
