# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
-------------------------------------------------
   File Name：     mongoClient.py
   Description :   封装MongoDB操作
   Author :        Goose-poet
   date：          2024/2/22
-------------------------------------------------
   Change Activity:
                   2024/02/22:
                   2024/02/23: 
-------------------------------------------------
"""
__author__ = 'goose-poet'
from pymongo.errors import ExecutionTimeout, ConnectionFailure, OperationFailure
from handler.logHandler import LogHandler
from random import choice
from pymongo import MongoClient as MongoConnect
import json


class MongodbClient(object):
    """
    MongoDB client

    DB中代理存放的结构为hash：
    key为代理的ip:por, value为代理属性的字典;
    """

    def __init__(self, **kwargs):
        """
        init
        :param host: host
        :param port: port
        :param password: password
        :return:
        """
        self.name = "use_proxy"
        kwargs.pop("username")
        kwargs.pop("db")
        self.__conn = MongoConnect(serverSelectionTimeoutMS=5000, socketTimeoutMS=5000, **kwargs)['proxy_pool']

    def get(self, https):
        """
        从hash中随机返回一个代理
        :return:
        """
        # if https:
        #     items_dict = self.__conn.hgetall(self.name)
        #     proxies = list(filter(lambda x: json.loads(x).get("https"), items_dict.values()))
        #     return choice(proxies) if proxies else None
        # else:
        #     proxies = self.__conn.hkeys(self.name)
        #     proxy = choice(proxies) if proxies else None
        #     return self.__conn.hget(self.name, proxy) if proxy else None
        if https:
            pipeline = [
                {'$match': {'https': True}},  # 添加查询条件
                {'$sample': {'size': 1}}  # 随机选择一个文档
            ]
        else:
            pipeline = [
                {'$sample': {'size': 1}}  # 随机选择一个文档
            ]
        result = list(self.__conn[self.name].aggregate(pipeline))
        # result[0].pop("_id") if result else None
        return json.dumps(result[0]) if result else None

    def put(self, proxy_obj):
        """
        将代理放入hash
        :param proxy_obj: Proxy obj
        :return:
        """
        filter_query = {'_id': proxy_obj.proxy}
        update_data = {'$set': proxy_obj.to_dict}
        result = self.__conn[self.name].update_one(filter_query, update_data, upsert=True).acknowledged
        return result

    def pop(self, https):
        """
        顺序弹出一个代理
        :return: proxy
        """
        if https:
            return self.__conn[self.name].find_one_and_delete({'https': True})
        else:
            return self.__conn[self.name].find_one_and_delete({})

    def delete(self, proxy_str):
        """
        移除指定代理
        :param proxy_str: proxy str
        :return:
        """
        self.__conn[self.name].delete_one(proxy_str)

    def exists(self, proxy_str):
        """
        判断指定代理是否存在
        :param proxy_str: proxy str
        :return:
        """
        return True if self.__conn[self.name].find_one(proxy_str) else False

    def update(self, proxy_obj):
        """
        更新 proxy 属性
        :param proxy_obj:
        :return:
        """
        filter_query = {'_id': proxy_obj.proxy}
        update_data = {'$set': proxy_obj.to_dict}
        self.__conn[self.name].update_one(filter_query, update_data, upsert=True)

    def getAll(self, https):
        """
        字典形式返回所有代理
        :return:
        """
        proxies = self.__conn[self.name].find()

        if https:
            return [json.dumps(proxy) for proxy in proxies if proxy.get("https")]
        else:
            return [json.dumps(proxy) for proxy in proxies]

    def clear(self):
        """
        清空所有代理
        :return:
        """
        return self.__conn[self.name].delete_many({})

    def getCount(self):
        """
        返回代理数量
        :return:
        """
        total_count = self.__conn[self.name].count_documents({})
        https_count = self.__conn[self.name].count_documents({"https": True})
        return {'total': total_count, 'https': https_count}

    def changeTable(self, name):
        """
        切换操作对象
        :param name:
        :return:
        """
        self.name = name

    def test(self):
        log = LogHandler('mongodb_client')
        try:
            self.getCount()
        except ExecutionTimeout as e:
            log.error('mongodb connection time out: %s' % str(e), exc_info=True)
            return e
        except ConnectionFailure as e:
            log.error('mongodb connection error: %s' % str(e), exc_info=True)
            return e
        except OperationFailure as e:
            log.error('mongodb connection error: %s' % str(e), exc_info=True)
            return e
