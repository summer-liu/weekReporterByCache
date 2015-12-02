# _*_ coding: utf8 _*_

# 周报第一部分，计算视频播放总数
# 正确度 95%
# TODO： 输入格式好看点

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

def playVideoWithDateRange(startDate, endDate, fromEnd, event):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "from": fromEnd,
            "eventKey": {"$in": event}
        }}
    ]
    return list(points.aggregate(pipeLine))

ios = "ios"
iosEvent = ["EnterMoviePlayerVC"]

android = "android"
androidEvent = ["playTaskVideo", "platGuideVideo"]
androidOldVersion = ["openVideo"]

pc = "pc"
pcEvent = ["openVideo"]

print(ios + " 当周视频播放总数：")
iosNum = (len(playVideoWithDateRange(startDate, endDate, ios, iosEvent)))
print(iosNum)

print(android + " 当周视频播放总数：")
androidNum = (len(playVideoWithDateRange(startDate, endDate, android, androidEvent)))
print(androidNum)

print(android + " Old Version 当周视频播放总数：")
androidOldNum = (len(playVideoWithDateRange(startDate, endDate, android, androidOldVersion)))
print(androidOldNum)

print(pc + " 当周视频播放总数：")
pcNum = (len(playVideoWithDateRange(startDate, endDate, pc, pcEvent)))
print(pcNum)

print("Total Play Video Times:")
print(iosNum + androidNum + androidOldNum + pcNum)
