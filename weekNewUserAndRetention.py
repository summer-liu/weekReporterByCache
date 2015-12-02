# _*_ coding: utf8 _*_

# 周活跃
# 正确性 60%
# TODO: 找真实数据测试 留存率算法 pc 以及mobile

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

#
# ###### Part.5 周留存 ######
# PC 上周新增(周内注册并激活)用户：
# Mobile 上周新增用户
# PC 本周留存:
# Mobile 本周留存:
# PC 本周新增：
# Mobile 本周新增:
# ###### Part.5 周留存 ######
# ----------------------------------------------------------------------------

def pcWeekNewUsers(startDate, endDate):
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
    pcWeekNewUsersList = list(userAttr.aggregate(pipeLine))
    if pcWeekNewUsersList:
        return pcWeekNewUsersList[0]['users']
    else:
        return []

print("pc this week new users:")
print(len(pcWeekNewUsers(startDate,endDate)))
print("pc last week new users:")
print(len(pcWeekNewUsers(lastWeekStartDate, lastWeekEndDate)))

def iosWeekNewUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "activatedTime": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered" : True,
            "os.ios": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    iosWeekNewUsersList = list(userAttr.aggregate(pipeLine))
    if iosWeekNewUsersList:
        return iosWeekNewUsersList[0]['users']
    else:
        return []

def androidWeekNewUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "activatedTime": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered" : True,
            "os.android": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    androidWeekNewUsersList = list(userAttr.aggregate(pipeLine))
    if androidWeekNewUsersList:
        return androidWeekNewUsersList[0]['users']
    else:
        return []

# print("ios this week new users:")
iosWeekNewUsersList = iosWeekNewUsers(startDate, endDate)
# print(len(iosWeekNewUsersList))
#
# print("android this week new users:")
androidWeekNewUsersList = androidWeekNewUsers(startDate, endDate)
# print(len(androidWeekNewUsersList))

print("Mobile end this week new users:")
print(len(iosWeekNewUsersList) + len(androidWeekNewUsersList))


def pcWeekRetention(lastWeekStartDate, lastWeekEndDate, startDate, endDate):
    pcLastWeekNewUsersList = pcWeekNewUsers(lastWeekStartDate, lastWeekEndDate)
    print(pcLastWeekNewUsersList)
    pipeLine = [
        {"$match": {
            {"recentSession": {
                "$gte": startDate,
                "$lt": endDate
            }},
            {"userId": {"$in": pcLastWeekNewUsersList}}
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    pcWeekRetentionList = list(userAttr.aggregate(pipeLine))
    if pcWeekRetentionList:
        return pcWeekRetentionList[0]['users']
    else:
        return []

print("pc week retention:")
print(len(pcWeekRetention(lastWeekStartDate, lastWeekEndDate, startDate, endDate)))

# def mobileWeekRetention(arg):
#     pipeLine = [
#     ]
#     return list(userAttr.aggregate(pipeLine))
