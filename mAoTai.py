#!/usr/bin/env python
#-*- coding:utf-8 -*-
# author:Administrator
# datetime:2019/8/15 14:10
# software: PyCharm

import sys,io
import datetime
import re
import logging
import threading
from threading import Lock
import json

import requests


sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

class MaoTaiSPider(object):

    '''
    茅台产品信息爬虫
    '''
    spider_name = 'maoTai'
    pingpai_list = [
                    "习酒",
                    "茅台醇",
                    "贵州茅台酒",
                    "黔茅",
                    "白金酒",
                    "赖茅馆"
                    ]
    brand_list__url = 'https://i.emaotai.cn/smartsales-trade-application/api/v1/smartsales/trade/mall/index/brand/list'
    product_url = 'https://i.emaotai.cn/smartsales-search-application/api/v1/smartsales/search/item?appCode=1&_t1565840859293'
    brand_infos = []   #品牌id
    product_ids = []  #商品id
    spider_id = 0
    lock = Lock()

    def __init__(self):
        self.__logging()
        self.f = open('maoTai.txt','w+',encoding='utf-8')

    def __logging(self):
        # FORMAT = "%(asctime)s %(thread)d %(message)s"
        FORMAT = "%(asctime)s %(message)s"
        logging.basicConfig(level=logging.INFO,
                            format=FORMAT,
                            datefmt="[%Y-%m-%d %H:%M:%S]")
        self.Logger = logging.getLogger('maoTai')
        self.Logger.setLevel(level=logging.INFO)
        self.Logger.info('开始运行')

    def get_brand_info(self):
        #获取系列id，并删选目标系列存入队列
        self.Logger.info('开始下载品牌id')
        resp = self.down_request(self.brand_list__url).text
        data = json.loads(resp)['data']
        if data:
            pass
        else:
            self.Logger.info('品牌信息获取出错')
            return
        for row in data:
            if row["name"] in self.pingpai_list:
                self.brand_infos.append({"brandId":row["brandId"],'name':row['name']})

        self.Logger.info('品牌id获取完成:{}'.format(self.brand_infos))

    def get_product_info(self,brand_info):
        #获取商品详情
        data = {
            'pageNum': '1',
            'pageSize': '1000',
            'keyword': brand_info['name'],
            'sortOrder': 'DESC',
            'sortType': '0'
        }
        #获取品牌商品列表
        resp = self.down_request(url=self.product_url,data=data,method='POST').text
        infos = json.loads(resp)['data']['pageInfo']['list']
        for info in infos:
            goods_data = {
                'itemId': info['itemId'],
                'skuId': info['skuId'],
                'shopId': info['shopId'],
                'appCode': '1'
            }
            good_url = 'https://www.emaotai.cn/smartsales-b2c-web-pc/details/{}-{}.html?skuId={}'.format(goods_data['itemId'],goods_data['shopId'],goods_data['skuId'])

            detail_url = 'https://i.emaotai.cn/yundt-application-trade-core/api/v1/yundt/trade/item/detail/get'
            headers = {
                'channelCode': '01',
                'channelId': '01',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Mobile Safari/537.36'
            }
            #获取商品详情内容
            resp2 = self.down_request(url=detail_url,data=data,method='GET',params=goods_data,headers=headers).text
            resp.encode('utf-8').decode("unicode-escape")
            items = json.loads(resp2)['data']
            pro = items['prodPropDtos']
            name = items['itemName']
            flavor = None
            if pro:
                for key in pro:
                    if '香型' in key['name']:
                        flavor = key['value']
                        break
            with self.lock:
                self.spider_id += 1
            item = {
                    'brand_name': brand_info['name'],
                    'goods_name': items['itemName'],
                    'flavor': flavor,
                    'volume': re.findall('\d+m*[l,L][*,x]*\d*[*,x]*\d*',name)[0] if re.findall('\d+m*[l,L][*,x]*\d*[*,x]*\d*',name) else None,
                    'alcohol_level' : re.findall('\d+%vol',name)[0] if re.findall('\d+%vol',name) else None,
                    '1919_url': good_url,
                    '1919_dotime': str(datetime.datetime.now()),
                    '1919_source': '茅台官网',
                    'sipder_id': str(self.spider_id)
                }
            self.save_data(item)
            self.Logger.info('{} :保存完毕'.format(item))

    def save_data(self,item):
        # item = json.dumps(item,ensure_ascii='utf8')
        item = str(item)
        self.f.write(item)

    def down_request(self,url,method = 'GET',data=None,params=None,headers=None):
        #下载配置，加代理可加载此处
        if headers:
            Headers=headers
        else:
            Headers = {
                'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Mobile Safari/537.36'
        }
        if method=='GET':
            try:
                resp = requests.get(url=url,headers = Headers,params=params)
                return resp
            except Exception as e:
                self.Logger.info('{}下载出错，响应码：{}'.format(url,resp.status_code))
        else:
            try:
                resp = requests.post(url=url, headers=Headers,data=data)
                return resp
            except Exception as e:
                self.Logger.info('{}下载出错，响应码：{}'.format(url, resp.status_code))

    def main(self):
        self.get_brand_info()
        T = []
        self.Logger.info('开始获取商品详情>>>')
        for brand_info in self.brand_infos:
            th = threading.Thread(target=self.get_product_info,args=(brand_info,))
            T.append(th)
        for th in T:
            th.start()
        for th in T:
            th.join()
        self.f.close()
        self.Logger.info('下载完毕，一共下载{}条数据'.format(self.spider_id))




if __name__ == '__main__':
    m = MaoTaiSPider()
    m.main()