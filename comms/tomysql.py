import pandas as pd
import pymongo
from sqlalchemy import create_engine


def drop_repeat(df, keep='last', inpalce=True):
    '''
    去除重复数据
    :param df: 要去重的Dataframe对象
    :param keep: 'first'保留重复项第一个，'last'保留重复项最后一个，'fail'删除所有重复数据
    :param inpalce: 'True'作用于原对象，'False'不作用于原对象
    :return: 返回DF去重之后的DF对象
    '''
    df.drop_duplicates(keep=keep, inplace=inpalce)
    return df


def drop_null(df, *args):
    '''
    去重df中一列或多列字段为空的数据
    :param df: Dataframe数组
    :param args: 要去除的列
    :return: 返回去除之后的df
    '''
    cols = args  # 接收要去空的列
    for col in cols:
        no = df[col].isnull() | df[col].apply(lambda x: str(x) == '') | df[col].apply(lambda x: str(x).isspace()) | df[col].apply(lambda x: str(x) == '  ')
        df = df[~no]
    return df


def read_mysql(sql, engine=create_engine('mysql+pymysql://root:123456@localhost:3306/spider_resource')):
    '''
    读取当前引擎库下的表数据返回Dataframe对象
    :param sql: 查询sql语句
    :param engine: 你要的数据的引擎
    :return: 返回Dataframe对象
    '''
    df = pd.read_sql_query(sql, engine)
    return df


def read_mongo(db_name, db_tables):
    '''
    从指定的mongo库中读取数据转化成Dataframe对象
    :param db_name: 数据库名
    :param db_tables: 表名/集合名
    :return: 返回Dataframe对象
    '''
    client = pymongo.MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1', )
    db = client[db_name][db_tables]
    df = pd.DataFrame(data=db.find())
    return df


def to_mysql(df, db_name, engine=create_engine('mysql+pymysql://root:123456@localhost:3306/spider_resource'), if_exists='append', index=False):
    '''
    将df转储到mysql数据库
    :param df: df数组
    :param db_name: 转储到的数据库名称
    :param engine: mysql引擎默认保存到本地的spider_resource库中
    :param if_exists: 存入方式：'fail'如果表存在什么也不做，'append'如果表存在追加到表中，不存在则直接建表插入，'replace'：如果表存在删除原表重新插入
    :param index: 是否插入下表
    '''
    df.to_sql(db_name, engine, if_exists=if_exists, index=index)
    print('数据以保存到mysql数据库')


if __name__ == '__main__':
    # df = to_mysql(read_mongo('all_comms', 'comms'), 'post_comms', engine = create_engine('mysql+pymysql://bd_ceshi:BDqilingzhengfan1@120.92.77.36:48368/spider_resource'))



    # df1 = read_mongo('red_book', 'video_id').values.tolist()
    # df2 = read_mongo('red_book', 'video_url').values.tolist()
    # for i in df1:
    #     if i in df2:
    #         df2.remove(i)
    # client = pymongo.MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1', ).red_book.video_fail
    # client1 = pymongo.MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1', ).red_book.post_data
    # print(df2, len(df2))
    # for i in df2:
    #     data = client1.find({'_id': i[0]}, {'video': 1})
    #     for i in data:
    #         print(i)
    #         id, url = i['_id'], i['video']
    #         client.save({'_id': id, 'url': url})
    client = pymongo.MongoClient('120.92.49.240', username='root', password='BDqilingzhengfan1', )
    # db = client.alluser.user_info1
    # db1 = client.alluser.user_info2
    # data = db.find()
    # for i in data:
    #     if str(i['gender']) == '2':
    #         db1.save({'_id': i['_id'], 'gender': '0', 'desc': i['desc'], 'created_time': i['created_time'], 'vip': i['vip'], 'source': i['source']})


    # db1 = client.red_book.post_data
    # db2 = client.red_book.pic_url
    # datas = [i['_id'] for i in db1.find({}, {'type': 0})]
    # print(datas)
    # for i in datas:
    #     print(i)
    #     db2.save({'_id': i})

    # sql = 'select id from weibo_user'
    # df = read_mysql(sql).values.tolist()
    # for i in df:
    #     client.weibo.user_id.save({'_id': i[0]})




    # red_book已经下载完成的图片处理
    # df1 = read_mongo('red_book', 'pic_over_id').values.tolist()
    # df1 = sum(df1, [])
    # for i in df1:
    #     print(i)
    #     client.red_book.pic_post_over.update({'_id': i}, {'$set': {'del_status': 1}})



    # mongo数据库数据合并
    # print(1)
    # df1 = read_mongo('red_book', 'pic_post_over')
    # print(2)
    # df2 = read_mongo('red_book', 'video_post_over')
    # print(3)
    # df = pd.concat([df1, df2])
    # print(df)
    # df.drop_duplicates(['title', 'description'], inplace=True)
    # df.to_sql('all_datas')

    # 把所有over的ID存储到mysql
    # to_mysql(drop_repeat(pd.concat([read_mongo('red_book', 'video_id_new'), read_mongo('red_book', 'pic_over_id')])), 'over_id')
    # db = client.all_post.post_data
    # for i in sum(drop_repeat(pd.concat([read_mongo('red_book', 'video_id_new'), read_mongo('red_book', 'pic_over_id')])).values.tolist(), []):
    #     print(i)
    #     db.update({'_id': i}, {'$set': {'download_status': 1}})

    # db = client.all_post.post_data
    # for i in read_mongo('bilibili', 'upload_id').values.tolist():
    #     print(i)
    #     db.update({'_id': i}, {'$set': {'download_status': 1}})

    print(1)
    # to_mysql(read_mongo('bilibili', 'datas'), 'bilibili_finish_time')
    # to_mysql(read_mongo('alluser', 'user_info1'), 'post_user')
    to_mysql(read_mongo('all_comms', 'comms'), 'post_comms')


    pass
