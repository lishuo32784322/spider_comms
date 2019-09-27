# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WeiboCommentsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id,count,text,created_time,like_count = scrapy.Field(),scrapy.Field(),scrapy.Field(),scrapy.Field(),scrapy.Field()
