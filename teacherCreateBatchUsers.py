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
rooms = db['rooms']
userAttr = db['userAttr']

# configure daterange
startDate = datetime.datetime(2015,7,16)
endDate   = startDate + datetime.timedelta(days = 2)

# 算出本周新来的教师用户
def calTeachersId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "role": "teacher",
            "rooms": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$_id",
            "rooms": "$rooms"
        }}
    ]
    return list(users.aggregate(pipeLine))

teacherIdAndRoomId = calTeachersId(startDate, endDate)

# 找出新来的教师新建的班级
roomsId = []
for room in teacherIdAndRoomId:
    roomsId.extend(room['rooms'])
# print("共创建教室:")
# print(len(roomsId))

#let all rooms id into room collections get all users
def allStuInRooms(roomsId):
    pipeLine = [
        {"$match": {
            "_id": {"$in": roomsId},
            "users": {"$exists": True, "$not": {"$size": 0}}
        }},
        {"$project": {
            "_id":"$users"
        }}
    ]
    return list(rooms.aggregate(pipeLine))

allStudents = allStuInRooms(roomsId)
# store all studentd id
flatAllstudents = []
for student in allStudents:
    flatAllstudents.extend(student['_id'])

print("本周教师创建批量用户数:")
teacherCreateStudentsNum = len(flatAllstudents)
print(teacherCreateStudentsNum)

# check flatAllstudents isin points

def stuInRoomAct(startDate, flatAllstudents):
    pipeLine = [
        {"$match": {
            "activatedTime": {
                "$gte": startDate
            },
            "userId": {"$in": flatAllstudents},
            "isActivated": True
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(userAttr.aggregate(pipeLine))

print("其中激活学生:")
teacherCreateStudentsNumActive = len(stuInRoomAct(startDate, flatAllstudents))
print(teacherCreateStudentsNumActive)
