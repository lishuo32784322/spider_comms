# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
import requests as r
import vthread
from pymongo import MongoClient
import urllib3
import chardet



urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
client = MongoClient('10.0.1.19', username='root', password='BDqilingzhengfan1',)


def get_ip():
    while 1:
        try:
            ip = random.choice([i['ip'] for i in client.ips.ips.find()])
            url = 'http://www.httpbin.org/ip'
            a, b = ip.split('://')
            proxies = {a: b}
            html = r.get(url, proxies=proxies, timeout=3)
            if '61.148.212.130' not in html.text:
                return ip
        except Exception as e:
            print(e)
            pass


@vthread.pool(5)
def get_comms(post_id, pcursor, photoId, this_count, break_num):
    print(post_id, pcursor, photoId, this_count, break_num)
    if this_count == break_num:
        return
    try:
        url = 'https://live.kuaishou.com/graphql'
        data = {"operationName": "commentListQuery",
                "variables": {"pcursor": pcursor, "photoId": photoId, "page": 1, "count": 20},
                "query": "query commentListQuery($photoId: String, $page: Int, $pcursor: String, $count: Int) {\n  shortVideoCommentList(photoId: $photoId, page: $page, pcursor: $pcursor, count: $count) {\n    commentCount\n    realCommentCount\n    pcursor\n    commentList {\n      commentId\n      authorId\n      authorName\n      content\n      headurl\n      timestamp\n      authorEid\n      status\n      subCommentCount\n      subCommentsPcursor\n      likedCount\n      liked\n      subComments {\n        commentId\n        authorId\n        authorName\n        content\n        headurl\n        timestamp\n        authorEid\n        status\n        replyToUserName\n        replyTo\n        replyToEid\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
        html = r.post(url, json=data, headers=headers, verify=False, proxies={'https': get_ip()}, timeout=3)
        comms = html.json(encoding=chardet.detect(html.content)['encoding'])['data']['shortVideoCommentList']
        get_pcursor = comms['pcursor']
        if str(get_pcursor) != 'no_more':
            this_count += 1
            get_comms(post_id, get_pcursor, photoId, this_count, break_num)
        commentList = comms['commentList']
        for comm in commentList:
            try:
                client.all_comms.new_comms.insert({"_id": str(comm['commentId']), 'comms_id': str(comm['commentId']), 'comms_user_id': str(comm['authorId']), 'comms_post_id': str(post_id), 'comms_like': int(comm['likedCount']), 'comms_text': str(comm['content']), 'source': 5, 'comms_time': str(comm['timestamp'])[:-3], 'export_time': 0})
            except:
                pass
    except Exception as e:
        print(e)
        get_comms(post_id, pcursor, photoId, this_count, break_num)
        # client.all_comms.err_comms.save({'post_id': post_id, 'pcursor': pcursor, 'photoId': photoId, 'this_count': this_count, 'status': 0})

while 1:
    datas = client.all_post.post_data.find({'source': 5, 'del_status': 0}, {'share_info': 1, 'origin_post_id': 1, '_id': 0}).limit(20)
    if datas is None:
        break
    for i in datas:
        client.all_post.post_data.update({'origin_post_id': i['origin_post_id']}, {'$set': {'del_status': 1}})
        get_comms(i['origin_post_id'], '', i['share_info'].split('photoId=')[1], 0, random.randint(10, 20))
    print('sleep_60s')
    time.sleep(60)
