# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
import pandas as pd
import pymongo
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool
from sqlalchemy import create_engine


client = pymongo.MongoClient('10.0.1.19', username='root', password='BDqilingzhengfan1')
pool = Pool(50)


def comm_to_mysql():
    engine = create_engine('mysql+pymysql://admin:BDqilingzhengfan1@10.0.1.20:3306/spider_resource')
    export_time = str(time.time()).split('.')[0]
    df_list = []

    def f(data):
        client.all_comms.new_comms.update({'_id': data['_id']}, {'$set': {'export_time': export_time}})
        df_list.append(
            [data['comms_id'], data['comms_like'], data['comms_user_id'], data['comms_post_id'], data['comms_text'],
             data['source'], data['comms_time']])

    pool.map(f, client.all_comms.new_comms.find({'export_time': 0}).limit(100000))
    df = pd.DataFrame(df_list,
                      columns=['comms_id', 'comms_like', 'comms_user_id', 'comms_post_id', 'comms_text', 'source',
                               'comms_time'])
    pattern = '@|\(.*?\)|\[.*?\]|点击字幕下面看完整版|下边有完整版哦，点一下就可以了|记得更新抖音，点完整版|抖音小助手|直播已经结束了吗？|快手创业|快手号|感谢快手官大大送上热门|我要涨粉丝|热门|快手官方正能量|爱爱热门|大大给的每一次热门|我要上热门|我爱官方|我要上热门|官方大大|感谢快手我要上热门|直播号|直播号\(\/d+\)|点击直播号进,快手官方！|爱官方爱热门|➕一下..*?️号店\d+|快手官方大大|双击加关注！|快手小助手|关注啦啦有奖中|速度双击➕关注|感谢快手平台,继续送福利！|我在直播间等你们|喜欢的宝宝➕主页vv|点点小爱心|双击关注点一下|快手热门|感谢官方|正在直播|喜欢的宝贝扣1|直播中速度进|内容|vx多少'
    df['comms_text'] = df['comms_text'].map(lambda x: re.sub(pattern, '', x.replace('抖音', '百搭').replace('直播', '更新').replace('抖主', '博主').replace('主播', '博主').replace('开播开播', '更新').replace('老铁们', '搭友们').replace('老铁粉', '搭友们').replace('铁粉', '搭友们').replace('不锈钢粉', '搭友们').replace('快手', '百搭')))
    no = (df['comms_text'].isnull() | df['comms_text'].apply(lambda x: str(x) == '') | df['comms_text'].apply(lambda x: str(x).isspace())| df['comms_text'].apply(lambda x: len(x) <= 2))
    df = df[~no]
    df.to_sql('post_comms', engine, if_exists='append', index=False)


if __name__ == '__main__':
    count = 0
    while 1:
        print(count,int(time.time()))
        count += 1
        comm_to_mysql()
        time.sleep(3600)


