import pandas as pd
import pymongo
from sqlalchemy import create_engine
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool
import time


export_time = str(time.time()).split('.')[0]
client = pymongo.MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1')
# engine = create_engine('mysql+pymysql://admin:BDqilingzhengfan1@10.0.1.20:3306/spider_resource')


# def f(comms_id):
#     client.all_comms.new_comms.update({'_id': comms_id[0]}, {'$set': {'export_time': export_time}})
#
#
#
# def mongo_to_mysql():
#     sql = 'select comms_id from post_comms'
#     df = pd.read_sql(sql, engine).values.tolist()
#     pool = Pool(100)
#     pool.map(f, df)
#
#
#
# mongo_to_mysql()


l = []
def f(data):
    l.append(data['_id'])

pool = Pool(500)
pool.map(f, client.all_comms.new_comms.find({'export_time': 0}))
print(len(l))
