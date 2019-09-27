from pymongo import MongoClient
import json


client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')
db = client.red_book.comms
db1 = client.all_comms.comms

datas = db.find({}, {'comms': 1, '_id': 0})
for i in datas:
    try:
        i = json.loads(i['comms'].replace('\'', '"'))['comments']
    except:i=''
    if i :
        for j in i:
            print(j)
            comms_id = j['id']
            comms_user_id = j['user']['id']
            comms_post_id = j['id']
            comms_like = j['likes']
            comms_text = j['content']
            comms_time = j['time']
            # print(comms_id, comms_user_id, comms_post_id, comms_like, comms_text, comms_time)
            try:
                db1.insert({'_id': comms_id,'source': 2,'comms_id': comms_id, 'comms_user_id': comms_user_id, 'comms_post_id': comms_post_id, 'comms_like': comms_like, 'comms_text': comms_text, 'comms_time': comms_time})
            except Exception as e:print(e)
