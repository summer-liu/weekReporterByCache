# _*_ coding: utf8 _*_

# 周报第五部分，计算视频播放总数
# 正确度 95%
# TODO： null

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://10.8.8.111:27017')
db = dbClient['matrix-prod25']

# open collections
points = db['points']
users = db['users']

# configure daterange
startDate = datetime.datetime(2015,11,15)
endDate   = startDate + datetime.timedelta(days = 2)

eventKeyList = [
    "enterHome",
    "clickSignup",
    "enterSignup",
    "clickSubmitSignup",
    "tempSignUpPost",
    "tempSignUpGetMe",
    "openVideo",
    "finishVideo",
]

def enterHome(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": "enterHome"
        }},
        {"$group": {
            "_id": 0,
            "users": {"$addToSet": "$user"}
        }}
    ]
    return list(points.aggregate(pipeLine))[0]['users']

print("enterHome: ")
userEnterHomeList = enterHome(startDate, endDate)
print(len(userEnterHomeList))

def funnelCalculate(startDate, endDate, eventKey, userList):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": eventKey,
            "user": {"$in": userList}
        }},
        {"$group": {
            "_id": 0,
            "users": {"$addToSet": "$user"}
        }}
    ]
    return list(points.aggregate(pipeLine))[0]['users']

print("clickSignup:")
clickSignupUserList = funnelCalculate(startDate, endDate, "clickSignup", userEnterHomeList)
print(len(clickSignupUserList))

print("enterSignup:")
enterSignupUserList = funnelCalculate(startDate, endDate, "enterSignup", clickSignupUserList)
print(len(enterSignupUserList))

print("clickSubmitSignup:")
clickSubmitSignupUserList = funnelCalculate(startDate, endDate, "clickSubmitSignup", enterSignupUserList)
print(len(clickSubmitSignupUserList))

print("tempSignUpPost:")
tempSignUpPostUserList = funnelCalculate(startDate, endDate, "tempSignUpPost", clickSubmitSignupUserList)
print(len(tempSignUpPostUserList))

print("tempSignUpGetMe:")
tempSignUpGetMeUserList = funnelCalculate(startDate, endDate, "tempSignUpGetMe", tempSignUpPostUserList)
print(len(tempSignUpGetMeUserList))

print("openVideo:")
openVideoUserList = funnelCalculate(startDate, endDate, "openVideo", tempSignUpGetMeUserList)
print(len(openVideoUserList))

print("finishVideo:")
finishVideoUserList = funnelCalculate(startDate, endDate, "finishVideo", openVideoUserList)
print(len(finishVideoUserList))
