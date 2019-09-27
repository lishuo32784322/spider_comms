from pymongo import MongoClient
import json
import time


client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')
db = client.bilibili.comms
db1 = client.all_comms.comms

datas = db.find({}, {'comms': 1, '_id': 0})
for i in datas:
    try:
        i = json.loads(i['comms'])['data']['replies']
    except:i=''
    if i:
        for j in i:
            # print(j)
            comms_id = j['rpid']
            comms_user_id = j['mid']
            comms_post_id = j['oid']
            comms_like = j['like']
            comms_text = j['content']['message']
            comms_time = j['ctime']
            timeArray = time.localtime(comms_time)
            comms_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
            try:
                db1.insert({'_id':comms_id ,'source': 1,'comms_id': comms_id, 'comms_user_id': comms_user_id, 'comms_post_id': comms_post_id, 'comms_like': comms_like, 'comms_text': comms_text, 'comms_time': comms_time})
            except Exception as e:print(e)
