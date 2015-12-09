# _*_ coding: utf8 _*_

from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime

# link to db
dbClient = MongoClient('mongodb://localhost:27017')
db = dbClient['yangcong-prod25']

# open collections
points = db['points']
users = db['users']
rooms = db['rooms']

# configure daterange

lastWeekStartDate = datetime.datetime(2015,11,8)
lastWeekEndDate   = lastWeekStartDate + datetime.timedelta(days = 7)

startDate = datetime.datetime(2015,11,15)
endDate   = startDate + datetime.timedelta(days = 7)

print("2015-11-15 ~ 2015-11-21")
print("")

print("###### Part.0 视频播放总数 ######")

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
androidEvent = ["playTaskVideo", "platGuideVideo"]  #play
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

print("视频播放总数：")
print(iosNum + androidNum + androidOldNum + pcNum)

print("###### Part.0 视频播放总数 ######")

print("###### Part.1 总用户数 ######")

# 用户累计新增人数 首次在洋葱数学网站内产生事件的用户数量
def calNewUserIn():
    pipeLine = [
        {"$match": {
            "from": {"$ne": ['ios', 'android']}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

newUsersList = calNewUserIn()
newUsersArray = []
for x in newUsersList:
    newUsersArray.append(x['_id'])
# print("PC 累计活动用户数：")
# print(len(newUsersArray))

def calRegUsers():
    pipeLine = [
        {"$match": {
            "usefulData.from": "signup"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

regUsersList = calRegUsers()

regUsersArray = []
for x in regUsersList:
    regUsersArray.append(x['_id'])
# print("PC 累计注册用户数:")
# print(len(regUsersArray))

regActUsers = []
regActUsers = list(set.intersection(set(newUsersArray), set(regUsersArray)))
print("PC端 累计活跃用户数:")
print(len(regActUsers))
print("PC端 累计活跃用户数算法 ==> 取 PC端曾经产生过埋点信息的用户 与 PC端注册用户 的 交集")


# iOS共计注册
# db.users.find({"usefulData.from":"ios"}).count()
def calIosReg():
    pipeLine = [
        {"$match": {
            "usefulData.from": "ios"
        }},
        {"$project": {
            "_id":"$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("iOS注册用户:")
iosRegUserNum = len(calIosReg())
print(iosRegUserNum)

# Android共计注册
# db.users.find({"usefulData.from":"android"}).count()
def calAndroidReg():
    pipeLine = [
        {"$match": {
            "usefulData.from": "android"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("Android注册用户:")
androidUserNum = len(calAndroidReg())
print(androidUserNum)

print("移动端注册用户总数")
print(iosRegUserNum + androidUserNum)

print("移动端未注册用户")
print("算法 ==> 友盟总数 减去 移动端注册用户总数")
print("")

print("###### Part.1 总用户数 ######")
print("")

print("###### Part.2 新增用户数 ######")

def calAndroidRegThisWeek(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": "android"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("Android本周注册用户:")
numOfAndroidRegThisWeek = len(calAndroidRegThisWeek(startDate, endDate))
print(numOfAndroidRegThisWeek)

def calIosRegThisWeek(startDate, endDate):
    pipeLine = [
        {"$match" : {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": "ios"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))
print("iOS本周注册用户：")
numOfIosRegThisWeek = len(calIosRegThisWeek(startDate, endDate))
print(numOfIosRegThisWeek)

print("移动端本周注册")
print(numOfIosRegThisWeek + numOfAndroidRegThisWeek)

print("移动端未注册")
print("算法 ===> 友盟本周数量 - 移动端本周注册")
print("")

# TODO: QQ from
def calQqPlatformUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                    "$gte": startDate,
                    "$lte": endDate
            },
            "usefulData.q": "qqPlatform"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print('QQ平台用户:')
print(len(calQqPlatformUsers(startDate, endDate)))

def calOtherPlatformUsers(startDate,endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lte": endDate
            },
            "usefulData.q": {
                "$exists": True,
                "$ne":"qqPlatform"
            }
        }}
    ]
    return list(users.aggregate(pipeLine))

print('其它平台用户:')
print(len(calOtherPlatformUsers(startDate, endDate)))

# 用ObjectId拿出班级创建日期
# 提取出班级里的用户
# 选出激活的用户

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
            "createdBy": {
                "$gte": startDate
            },
            "user": {"$in": flatAllstudents}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("其中激活学生:")
teacherCreateStudentsNumActive = len(stuInRoomAct(startDate, flatAllstudents))
print(teacherCreateStudentsNumActive)

print("###### Part.2 新增用户数 ######")
print("")

print("###### Part.3 周活跃 ######")

def calPcWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "from": "pc"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

pcActUsers = calPcWork(startDate, endDate)
pcActUsersArray = []
for x in pcActUsers:
    pcActUsersArray.append(x['_id'])
# print("PC本周活动用户:")
# print(len(pcActUsersArray))

def isUsers(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": "signup"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

def isAllUsers():
    pipeLine = [
        {"$match": {
            "usefulData.from": "signup"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))


regUsers = isAllUsers()
regUsersArray = []
for x in regUsers:
    regUsersArray.append(x['_id'])
# print("PC本周注册用户")
# print(len(regUsersArray))

pcOutArray = []
pcOutArray = list(set.intersection(set(regUsersArray), set(pcActUsersArray)))
print("PC周活跃统计")
print(len(pcOutArray))

def calIosWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "ios",
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))

def calAndroidWork(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "from": "android"
        }},
        {
            "$group": {
                "_id": "$user"
            }
        }
    ]
    return list(points.aggregate(pipeLine))


mobileOutList = []
mobileOutList.extend(calIosWork(startDate, endDate));
mobileOutList.extend(calAndroidWork(startDate, endDate));
# print(len(mobileOutList))

mobileOutArray = []
for x in mobileOutList:
    mobileOutArray.append(x['_id'])
print("Mobile 周活跃总计")
print(len(mobileOutArray))

bothOutArray = []
bothOutArray = list(set.intersection(set(pcOutArray), set(mobileOutArray)))
print("两端同活跃统计")
print(len(bothOutArray))

print("###### Part.3 周活跃 ######")
print("")

print("###### Part.4 首页漏斗######")


def enterHomeUserId(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": "enterHome"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterHomeUserId = enterHomeUserId(startDate, endDate)

# processing allEnterHomeUserId as a array
enterHomeUserIdArray = []
for x in allEnterHomeUserId:
    enterHomeUserIdArray.append(x['_id'])
print("进入首页:")
print(len(enterHomeUserIdArray))


# clickSignup

def clickSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSignupUserId = clickSignupUserId(startDate, endDate, enterHomeUserIdArray)

clickSignupUserIdArray = []
for x in allClickSignupUserId:
    clickSignupUserIdArray.append(x['_id'])
print("点击注册: ")
print(len(clickSignupUserIdArray))

# enterSignup

def enterSignupUserId(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "enterSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allEnterSignupUserId = enterSignupUserId(startDate, endDate, clickSignupUserIdArray)

enterSignupUserIdArray = []
for x in allEnterSignupUserId:
    enterSignupUserIdArray.append(x['_id'])
print("进入注册页: ")
print(len(enterSignupUserIdArray))

# clickSubmitSignup

def clickSubmitSignup(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "clickSubmitSignup"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allClickSubmitSignupUserId = clickSubmitSignup(startDate, endDate, enterSignupUserIdArray)

clickSubmitSignupUserIdArray = []
for x in allClickSubmitSignupUserId:
    clickSubmitSignupUserIdArray.append(x['_id'])
print("点击提交注册: ")
print(len(clickSubmitSignupUserIdArray))

# tempSignUpPost

def tempSignUpPost(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "tempSignUpPost"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

alltempSignUpPostUserId = tempSignUpPost(startDate, endDate, clickSubmitSignupUserIdArray)

tempSignUpPostArray = []
for x in alltempSignUpPostUserId:
    tempSignUpPostArray.append(x['_id'])
print("tempSignUpPost:")
print(len(tempSignUpPostArray))

# tempSignUpGetMe
def tempSignUpGetMe(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "tempSignUpGetMe"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))
alltempSignUpGetMeUserId = tempSignUpGetMe(startDate, endDate, tempSignUpPostArray)

tempSignUpGetMeArray = []
for x in alltempSignUpGetMeUserId:
    tempSignUpGetMeArray.append(x['_id'])
print("注册成功(tempSignUpGetMe): ")
print(len(tempSignUpGetMeArray))

# openVideo

def openVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "openVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allopenVideoUserId = openVideo(startDate, endDate, tempSignUpGetMeArray)

openVideoArray = []
for x in allopenVideoUserId:
    openVideoArray.append(x['_id'])
print("打开视频: ")
print(len(openVideoArray))

# finishVideo

def finishVideo(startDate, endDate, userId):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": userId},
            "eventKey": "finishVideo"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

allfinishVideoUserId = finishVideo(startDate, endDate, openVideoArray)

finishVideoArray = []
for x in allfinishVideoUserId:
    finishVideoArray.append(x['_id'])
print("关闭视频: ")
print(len(finishVideoArray))

print("###### Part.4 首页漏斗######")


print("###### Part.5 周留存 ######")
# TODO:
def usersInLastWeek(startDate, endDate):
    pipeLine = [
        {"$match": {
            # "from":"pc",
            "createdBy": {
                "$gte": startDate,
                "$lt":  endDate
            }
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

usersInLastWeekList = usersInLastWeek(lastWeekStartDate, lastWeekEndDate)

usersInLastWeekArray = []
for x in usersInLastWeekList:
    usersInLastWeekArray.append(x['_id'])

# Mobile行动
def usersInIosAndAndroid(startDate, endDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "from": {"$in": ['ios', 'android']}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

usersInIosAndAndroidList = usersInIosAndAndroid(lastWeekStartDate, lastWeekEndDate)

# user id
usersInIosAndAndroidArray = []
for x in usersInIosAndAndroidList:
    usersInIosAndAndroidArray.append(x['_id'])

# round 2
# user or mobile users in user collection
def isUser(startDate, endDate):
    startDate = startDate
    endDate = endDate
    pipeLine = [
        {"$match": {
            # "usefulData.registDate": {
            #     "$gte": startDate,
            #     "$lt": endDate
            # },
            "usefulData.from": {"$in": ["batch","signup"]}
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

def isMobileUser(startDate, endDate):
    pipeLine = [
        {"$match": {
            "usefulData.registDate": {
                "$gte": startDate,
                "$lt": endDate
            },
            "usefulData.from": {"$in": ['ios', 'android']}
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

isUserList       = isUser(lastWeekStartDate, lastWeekEndDate)
isMobileUserList = isMobileUser(lastWeekStartDate, lastWeekEndDate)

# points活跃用户中的注册用户
isUserArray = []
for x in isUserList:
    isUserArray.append(x['_id'])

isMobileUserArray = []
for x in isMobileUserList:
    isMobileUserArray.append(x['_id'])

# round 3
# 真正的新增用户数量
# realNewUser = list(set(usersInLastWeekArray) - set(isUserArray))
realNewUser = []
realNewUser = list(set.intersection(set(usersInLastWeekArray), set(isUserArray)))
# 新增的注册用户
print("PC 上周新增(周内注册并激活)用户：")
print(len(realNewUser))

# realMobileNewUser = list(set(usersInIosAndAndroidArray) - set(isMobileUserArray))
realMobileNewUser = []
realMobileNewUser = list(set.intersection(set(usersInIosAndAndroidArray), set(isMobileUserArray)))
print("Mobile 上周新增用户")
print(len(realMobileNewUser))

# 上周新增，在本周是否产生行为
def isActThisWeek(startDate, endDate, testUser):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "user": {"$in": testUser}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

pcUser = isActThisWeek(startDate, endDate, realNewUser)
mobileUser = isActThisWeek(startDate, endDate, realMobileNewUser)

print("PC 本周留存:")
print(len(pcUser))
print("Mobile 本周留存:")
print(len(mobileUser))

usersInThisWeekList = usersInLastWeek(startDate, endDate)
mobileUsersInThisWeekList = usersInIosAndAndroid(startDate, endDate)

usersInThisWeekArray = []
for x in usersInThisWeekList:
    usersInThisWeekArray.append(x['_id'])

mobileUsersInThisWeekArray = []
for x in mobileUsersInThisWeekList:
    mobileUsersInThisWeekArray.append(x['_id'])

thisWeekIsUsers         = isUser(startDate, endDate)
thisWeekIsMobileUsers   = isMobileUser(startDate, endDate)

thisWeekIsUsersArray = []
for x in thisWeekIsUsers:
    thisWeekIsUsersArray.append(x['_id'])

thisWeekMobileUsersArray = []
for x in thisWeekIsMobileUsers:
    thisWeekMobileUsersArray.append(x['_id'])

thisWeekNewUser = []
thisWeekNewUser = list(set.intersection(set(usersInThisWeekArray), set(thisWeekIsUsersArray)))
print("PC 本周新增：")
print(len(thisWeekNewUser))

thisWeekNewMobileUser = []
thisWeekNewMobileUser = list(set.intersection(set(thisWeekMobileUsersArray), set(mobileUsersInThisWeekArray)))
print("Mobile 本周新增:")
print(len(thisWeekNewMobileUser))
print("###### Part.5 周留存 ######")

print("###### Part.6 ######")
print("H5页面")
print(">>>需要从线上获取数据 web_tracks_mobile")
print("###### Part.6 ######")
