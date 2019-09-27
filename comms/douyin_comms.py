import requests as r
from pymongo import MongoClient
import uuid
import vthread
import time
import random


client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1',)

@vthread.pool(5)
def get_comms2(post_id, page, num):
    print(post_id, page, num)
    if page > num:
        return
    try:
        url = f'http://api01.6bqb.com/douyin/comment?apikey=998C8229EE98DD732CC093968DBABF99&itemId={str(post_id)}&page={str(page)}'
        comms = r.get(url).json()
        for comm in comms['data']:
            post_id = comm['aweme_id']
            comm_user = comm['user']['uid']
            comm_id = uuid.uuid1() if str(comm['reply_id']) == '0' else str(comm['reply_id'])
            comm_like = comm['digg_count']
            comm_text = comm['text']
            print(comm_text)
            comm_time = comm['create_time']
            source = 8
            try:
                client.all_comms.douyin_allcomms.save({'comms': comms['data']})
                client.all_comms.new_comms.insert({'_id': str(comm_id), 'comms_id': str(comm_id), 'comms_user_id': str(comm_user), 'comms_post_id': str(post_id), 'comms_like': int(comm_like), 'comms_text': str(comm_text), 'source': source, 'comms_time': comm_time, 'export_time': 0})
            except Exception as e:
                print(e)
        page += 1
        if str(comms['hasNext']) == 'True':
            get_comms2(post_id, page, num)
    except:pass
#        client.all_comms.douyin_post_id.save({'_id': post_id, 'page': page, 'status': 0})


def get_comms(post_id, page, num):
    print(post_id, page, num)
    if page > num:
        return
    try:
        url = f'http://api01.6bqb.com/douyin/comment?apikey=998C8229EE98DD732CC093968DBABF99&itemId={str(post_id)}&page={str(page)}'
        comms = r.get(url).json()
        client.all_comms.douyin_post_id.update({'_id': post_id}, {'$set': {'status': 1}})
        for comm in comms['data']:
            post_id = comm['aweme_id']
            comm_user = comm['user']['uid']
            comm_id = uuid.uuid1() if str(comm['reply_id']) == '0' else str(comm['reply_id'])
            comm_like = comm['digg_count']
            comm_text = comm['text']
            print(comm_text)
            comm_time = comm['create_time']
            source = 8
            try:
                client.all_comms.douyin_allcomms.save({'comms': comms['data']})
                client.all_comms.new_comms.insert({'_id': str(comm_id), 'comms_id': str(comm_id), 'comms_user_id': str(comm_user), 'comms_post_id': str(post_id), 'comms_like': int(comm_like), 'comms_text': str(comm_text), 'source': source, 'comms_time': comm_time, 'export_time': 0})
            except Exception as e:
                print(e)
        page += 1
        if str(comms['hasNext']) == 'True':
            get_comms2(post_id, page, num)
    except:pass
#        client.all_comms.douyin_post_id.save({'_id': post_id, 'page': page, 'status': 0})


while 1:
    datas = client.all_comms.douyin_post_id.find({'status': 0}).limit(50)
    if datas is None:
        break
    for i in datas:
        get_comms(i['_id'], i['page'], random.randint(3, 10))
    print('sleep_60s')
    time.sleep(60)
