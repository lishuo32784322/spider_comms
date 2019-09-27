# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
import requests as r
import vthread
from pymongo import MongoClient
import urllib3
import chardet

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
client = MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1',)


def get_ip():
    while 1:
        try:
            ip = random.choice([i['ip'] for i in client.ips.ips.find()])
            url = 'http://www.httpbin.org/ip'
            a, b = ip.split('://')
            proxies = {a: b}
            html = r.get(url, proxies=proxies, timeout=3)
            if '120.131.10.99' not in html.text:
                return ip
        except:
            pass


@vthread.pool(50)
def get_comms(post_id, pcursor, photoId, this_count, count):
    if count == 3:
        return
    count += 1
    if this_count == 5:
        return

    try:
        url = 'https://live.kuaishou.com/graphql'
        data = {"operationName": "commentListQuery",
                "variables": {"pcursor": pcursor, "photoId": photoId, "page": 1, "count": 20},
                "query": "query commentListQuery($photoId: String, $page: Int, $pcursor: String, $count: Int) {\n  shortVideoCommentList(photoId: $photoId, page: $page, pcursor: $pcursor, count: $count) {\n    commentCount\n    realCommentCount\n    pcursor\n    commentList {\n      commentId\n      authorId\n      authorName\n      content\n      headurl\n      timestamp\n      authorEid\n      status\n      subCommentCount\n      subCommentsPcursor\n      likedCount\n      liked\n      subComments {\n        commentId\n        authorId\n        authorName\n        content\n        headurl\n        timestamp\n        authorEid\n        status\n        replyToUserName\n        replyTo\n        replyToEid\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n"}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
        html = r.post(url, json=data, headers=headers, verify=False, proxies={'https': get_ip()})
        comms = html.json(encoding=chardet.detect(html.content)['encoding'])['data']['shortVideoCommentList']
        comms = html.json()['data']['shortVideoCommentList']
        get_pcursor = comms['pcursor']
        commentList = comms['commentList']
        for comm in commentList:
            try:
                client.all_comms.new_comms.insert(
                    {"_id": str(comm['commentId']), 'export_time': 0,'comms_id': str(comm['commentId']),
                     'comms_user_id': str(comm['authorId']), 'comms_post_id': str(post_id),
                     'comms_like': int(comm['likedCount']), 'comms_text': str(comm['content']), 'source': 5, 'comms_time': str(comm['timestamp'])[:-3]})
            except:
                pass
        client.all_comms.err_comms.update({'post_id': post_id}, {'$set': {'status': 1}})
        if str(get_pcursor) != 'no_more':
            this_count += 1
            get_comms(post_id, get_pcursor, photoId, this_count, count)
    except Exception as e:
        print(e)
        try:
            client.all_comms.err_comms.insert({'post_id':post_id, 'pcursor': pcursor, 'photoId': photoId, 'this_count': this_count, 'count': count, 'status': 0})
        except Exception as e:
            print(e)
#        get_comms(post_id, pcursor, photoId, this_count, count)

while 1:
    datas = client.all_comms.err_comms.find({'status': 0})
    for data in datas:
        get_comms(data['post_id'],data['pcursor'],data['photoId'],data['this_count'],data['count'])
    time.sleep(3600)
