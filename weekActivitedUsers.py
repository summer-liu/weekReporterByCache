# _*_ coding: utf8 _*_

# 周活跃
# 正确性 60%
# TODO: 找真实数据测试 两端同活跃用户
# 调整两端同活跃用户算法

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


# ----------------------------------------------------------------------------
# 周活跃

def pcWeekActivedUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered": True,
            "os.pc": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    userList = list(userAttr.aggregate(pipeLine))
    if userList:
        return userList[0]['users']
    else:
        return []

def mobileWeekActivedUsersAndroid(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered": True,
            "os.android": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    userList = list(userAttr.aggregate(pipeLine))
    if userList:
        return userList[0]['users']
    else:
        return []

def mobileWeekActivedUsersIos(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "isRegistered": True,
            "os.ios": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    userList = list(userAttr.aggregate(pipeLine))
    if userList:
        return userList[0]['users']
    else:
        return []

pcWeekActivedUsersList = pcWeekActivedUsers(startDate, endDate)
mobileWeekActivedUsersIosList = mobileWeekActivedUsersIos(startDate, endDate)
mobileWeekActivedUsersAndroidList = mobileWeekActivedUsersAndroid(startDate, endDate)

print("本周PC端活跃用户：")
print(len(pcWeekActivedUsersList))
print("本周iOS端活跃用户：")
print(len(mobileWeekActivedUsersIosList))
print("本周Android端活跃用户：")
print(len(mobileWeekActivedUsersAndroidList))

def bothPcIos(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "os.pc": True,
            "os.ios": True
        }},
        {"$group": {
            "_id": None,
            "users": {"$addToSet": "$userId"}
        }}
    ]
    bothPcIosList = list(userAttr.aggregate(pipeLine))
    if bothPcIosList:
        return bothPcIosList[0]['users']
    else:
        return []

def bothPcAndroid(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "os.pc": True,
            "os.android": True
        }}
    ]
    bothPcAndroidList = list(userAttr.aggregate(pipeLine))
    if bothPcAndroidList:
        return bothPcAndroidList[0]['users']
    else:
        return []

def bothIosAndroid(startDate, endDate):
    pipeLine = [
        {"$match": {
            "recentSession": {
                "$gte": startDate,
                "$lt": endDate
            },
            "os.ios": True,
            "os.android": True
        }}
    ]
    bothIosAndroidList = list(userAttr.aggregate(pipeLine))
    if bothIosAndroidList:
        return bothIosAndroidList[0]['users']
    else:
        return []

bothPcIosList = bothPcIos(startDate, endDate)
bothPcAndroidList = bothPcAndroid(startDate, endDate)
bothIosAndroidList = bothIosAndroid(startDate, endDate)

print(bothPcIosList)
print(bothPcAndroidList)
print(bothIosAndroidList)

# bothActivedUsers = list(set(bothPcIosList.extend(bothPcAndroidList)) - set(bothIosAndroidList))
print("两端同活跃用户:")
# print(len(bothActivedUsers))

# 周活跃 end
