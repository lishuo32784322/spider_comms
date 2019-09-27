# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
import pymysql


class WeiboCommentsPipeline(object):


    def process_item(self, item, spider):
        id , text, created_time, like_count = item['id'], item['text'], item['created_time'], item['like_count']
        # self.collection.save({'id':id,'count':count,'text':text,'created_time':created_time,'like_count':like_count})
        sql = 'insert into weibo_allcomments values(%s,%s,%s,%s)'
        try:
            self.cursor.execute(sql, [id , text, created_time, like_count])
            print('MYSQL新增加一个数据')
        except Exception as e :
            self.collection.save({'_id':id,'text':text,'created_time':created_time,'like_count':like_count})
            print(e)
            print('MONGO新增加一个数据')



    def open_spider(self, spider):
        client = MongoClient('localhost', 27017)
        db = client.weibo
        self.collection = db.comm_datas
        self.connect = pymysql.connect(
            # host='120.92.77.36',  # 数据库地址
            host='localhost',  # 数据库地址
            # port=48368,  # 数据库端口
            port=3306,  # 数据库端口
            db='spider_resource',  # 数据库名
            # user='bd_ceshi',  # 数据库用户名
            user='root',  # 数据库用户名
            # passwd='BDqilingzhengfan1',  # 数据库密码
            passwd='123456',  # 数据库密码
            charset='utf8',  # 编码方式
            use_unicode=True)
        self.cursor = self.connect.cursor()
        self.connect.autocommit(True)
        print('爬虫已开启')

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
        print('爬虫已关闭')
