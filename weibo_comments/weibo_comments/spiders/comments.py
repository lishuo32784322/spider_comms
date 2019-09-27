# -*- coding: utf-8 -*-
import random
import re

import scrapy
from sqlalchemy import create_engine
import pandas as pd
import json
from weibo_comments.items import WeiboCommentsItem as Item

class CommentsSpider(scrapy.Spider):
    name = 'comments'
    start_urls = ['https://m.weibo.cn']

    def parse(self, response):
        # print(1)
        engine = create_engine('mysql+pymysql://root:123456@localhost:3306/spider_resource')
        sql = '''select comments_id from spider_weibo;'''
        df = pd.read_sql(sql, engine)
        ids = df['comments_id'].values.tolist()
        print(ids)
        for id in ids:
            url = 'https://api.weibo.cn/2/comments/build_comments?gsid=_2AkMruDoof8NhqwJRmfodyG3kaIlwzg3EieKd5MvzJRM3HRl-wT9jqk8ttRV6ADgQXrntcHiIEYzqcmRwOla2KjKa_Yx4&uid=1012846644745&wm=3333_2001&sensors_is_first_day=true&from=1095193010&sensors_device_id=C2995943-3341-43D3-B1B2-B792C23C7645&c=iphone&v_p=74&skin=default&v_f=1&networktype=wifi&b=0&s=5df77777&lang=zh_CN&ua=iPhone8,2__weibo__9.5.1__iphone__os10.3.3&sflag=1&ft=0&did=82b61892927feab5b65ef524ab9042c2&checktoken=9236d9f1ac111c55fb24c6b2c20d22dd&id=%s&mid=%s&since_id=0&trim_level=1&is_show_bulletin=2&count=20&luicode=10000198&fetch_level=0&featurecode=10000085&uicode=10000002&_status_id=%s&is_reload=1&is_mix=1&request_type=default&page=0&is_append_blogs=1&lfid=1076031002143827_-_WEIBO_SECOND_PROFILE_WEIBO_ORI&moduleID=feed&lcardid=1076031002143827_-_WEIBO_SECOND_PROFILE_WEIBO_ORI_-_%s'%(id,id,id,id)
            yield scrapy.Request(url,callback=self.parse1,dont_filter=True,meta={'id':id})


    def parse1(self,response):
        # print(response.url)
        item = Item()
        id = response.meta['id']
        html = json.loads(response.text)
        try:
            datas = html['root_comments']
        except:pass
        try:
            max_id = html['max_id']
            url = re.sub('&cum=(.*?)','',re.sub('&max_id=(.*?)&page=0&', max_id, response.url))
            yield scrapy.Request(url,callback=self.parse1)
        except Exception as e :print(e)
        try:
            for data in datas:
                created_time = data['created_at']
                text = data['text']
                if text:
                    like_count = data['like_counts']
                    mons = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    a, mon, day, hms, b, year = created_time.split(' ')
                    mon = mons.index(mon) + 1
                    created_time = year + '-' + str(mon) + '-' + day + ' ' + hms
                    item['id'],item['text'],item['created_time'],item['like_count']=id,text,created_time,like_count
                    # print(count,text,created_time,like_count)
                    yield item
        except:
            for data in html['datas']:
                # print(data)
                try:
                    created_time = data['data']['created_at']
                except:continue
                try:
                    text = data['data']['text']
                except:continue
                if text:
                    try:
                        like_count = data['data']['like_counts']
                    except:
                        like_count = 0
                    mons =['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    a, mon, day, hms, b, year = created_time.split(' ')
                    mon = mons.index(mon) + 1
                    created_time = year + '-' + str(mon) + '-' + day + ' ' + hms
                    item['id'], item['text'], item['created_time'], item['like_count'] = id, text, created_time, like_count
                    # print(text,created_time,like_count)
                    yield item


