#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:Administrator
# datetime:2019/8/15 14:10
# software: PyCharm
import logging
from threading import Lock
from threading import Thread
import datetime

import json
import requests
import fake_useragent
from queue import Queue

class WuLiangYeSpider(object):

    '''
    五粮液产品信息爬虫
    '''

    ua = fake_useragent.UserAgent()
    spider_name = 'wuNiangYe'
    all_series_url = 'https://www.wuliangye.com.cn/wly_eap/cn/api/v1/publix/product/category_list'
    product_list_url = 'https://www.wuliangye.com.cn/wly_eap/cn/api/v1/publix/products?category_id={}'
    product_detail_url = 'https://www.wuliangye.com.cn/wly_eap/cn/api/v1/publix/product/id/{}'
    series_infos_queue = Queue()  #系列信息
    goods_queue = Queue()  #商品列表
    spider_id = 0
    lock = Lock()

    def __init__(self):
        self.__logging()
        self.f = open('wuniangye.txt','w+')
        self.headers = self.get__global_headers()

    def __logging(self):
        FORMAT = "%(asctime)s %(thread)d %(message)s"
        # FORMAT = "%(asctime)s %(message)s"
        logging.basicConfig(level=logging.INFO,
                            format=FORMAT,
                            datefmt="[%Y-%m-%d %H:%M:%S]")
        self.Logger = logging.getLogger('maoTai')
        self.Logger.setLevel(level=logging.INFO)
        self.Logger.info('开始运行')

    def get_series_info(self):
        #获取系列信息
        resp = self.down_request(self.all_series_url).text.replace('null','None')
        series_list = eval(resp)
        for row in series_list:
            series_info = {
                'name':row['name'],
                'id':row['id'],
            }
            self.series_infos_queue.put(series_info)
        self.Logger.info('系列信息下载完成:{}'.format(self.series_infos_queue.qsize()))

    def get_product_info(self):
        #获取系列产品列表
        while True:
            if not self.series_infos_queue.empty():
                series_info = self.series_infos_queue.get()
                product_list_url = self.product_list_url.format(series_info['id'])
                resp = self.down_request(product_list_url).text.replace('null','None').replace('false','False').replace('true','True')
                jsons = eval(resp)
                for goods in jsons['products']:
                    goods_info = {
                        'category':str(goods['categoryId']),
                        'id': str(goods['id']),
                        'name': goods['name']
                    }
                    self.goods_queue.put(goods_info)
            else:
                # self.Logger.info('')
                return

    def get_product_detail_info(self):
        # 获取产品详细信息
        while True:
            if not self.goods_queue.empty():
                goods_info = self.goods_queue.get()
                product_detail_url = self.product_detail_url.format(goods_info['id'])
                resp = self.down_request(product_detail_url).text.replace('null','None').replace('false','False').replace('true','True')
                jsons = eval(resp)
                with self.lock:
                    self.spider_id += 1
                item = {
                        'brand_name': jsons['category'],
                        'goods_name': jsons['name'],
                        'flavor': jsons['spec']['flavor'],
                        'volume': str(jsons['spec']['netWeight']) + 'ml',
                        'alcohol_level': str(jsons['spec']['degree']) + '%vol',
                        '1919_url': 'https://www.wuliangye.com.cn/zh/main/main.html#/g=PRODUCT&id={}&dId={}'.format(jsons['categoryId'],jsons['id']),
                        '1919_dotime': str(datetime.datetime.now()),
                        '1919_source': '五粮液官网',
                        'sipder_Id': str(self.spider_id)
                    }
                self.save_data(item)
                self.Logger.info('{} :保存完毕'.format(item))
            else:
                self.Logger.info('下载系列列表完成')
                return

    def get__global_headers(self):
        self.Logger.info('获取全局headers中的token')
        headers= {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Access-Key': '3588939af15343d491b5b470be0964c5',
                # 'Access-Token': 'tiGrHIkgQuGq6bGjfnVupg',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json;charset=UTF-8',
                'Host': 'www.wuliangye.com.cn',
                'Referer': 'https://www.wuliangye.com.cn/zh/main/main.html',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': self.ua.random,
                'X-Requested-With': 'XMLHttpReques',
            }
        token_url = 'https://www.wuliangye.com.cn/wly_eap/cn/api/v1/publix/get_access_token'
        resp = requests.get(token_url,headers=headers).text
        headers['Access-Token'] = json.loads(resp)["access_token"]
        return headers

    def save_data(self,item):
        item = str(item)
        self.f.write(item)

    def down_request(self,url,method = 'GET',data=None,params=None,headers=None):
        #下载配置，加代理可加载此处
        if headers:
            Headers=headers
        else:
            Headers = self.headers
        if method=='GET':
            try:
                resp = requests.get(url=url,headers = Headers,params=params)
                return resp
            except Exception as e:
                self.Logger.info('{}下载出错'.format(url))
        else:
            try:
                resp = requests.post(url=url, headers=Headers,data=data)
                return resp
            except Exception as e:
                self.Logger.info('{}下载出错'.format(url))

    def main(self,th_num):
        self.get_series_info()
        self.get_product_info()
        th_list= []
        for th in range(th_num):
            th_list.append(Thread(target=self.get_product_detail_info,args=(),name='th name {}'.format(a)))
        for t in th_list:
            t.start()
        for t in th_list:
            t.join()
        self.Logger.info('下载完毕，一共下载{}条数据'.format(self.spider_id))


if __name__ == '__main__':
    m = WuLiangYeSpider()
    m.main(10)