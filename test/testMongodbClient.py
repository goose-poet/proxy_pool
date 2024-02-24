# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     testSsdbClient
   Description :
   Author :        JHao
   date：          2020/7/3
-------------------------------------------------
   Change Activity:
                   2020/7/3:
-------------------------------------------------
"""
__author__ = 'goose-poet'

import sys
sys.path.append("..")


def testSsdbClient():
    from db.dbClient import DbClient
    from helper.proxy import Proxy

    uri = "mongodb://localhost:27017"
    db = DbClient(uri)
    db.changeTable("test_proxy")
    proxy = Proxy.createFromJson(
        '{"proxy": "118.190.79.36:8090", "https": false, "fail_count": 0, "region": "", "anonymous": "", "source": "freeProxy14", "check_count": 4, "last_status": true, "last_time": "2021-05-26 10:58:04"}')

    print("put: ", db.put(proxy))

    print("get: ", db.get(https=None))

    print("exists: ", db.exists("118.190.79.36:8090"))

    print("exists: ", db.exists("27.38.96.101:9797"))

    print("exists: ", db.exists("27.38.96.101:8888"))

    print("getAll: ", db.getAll(https=None))

    print("delete: ", db.delete("118.190.79.36:8090"))

    print("pop: ", db.pop(https=None))

    print("clear: ", db.clear())

    print("getCount", db.getCount())


if __name__ == '__main__':
    testSsdbClient()
