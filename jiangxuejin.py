#!usr/bin/env python
#coding: utf-8

"""
从http://xszz.chsi.com.cn/gjjxj/下载获得奖学金
的名单
"""
import urllib2,urllib
from scrapy import Selector
import os

def get_response(url):
    """
    获得response并返回bytes型的
    """
    xtml = urllib2.urlopen(url)
    content = xtml.read()
    response = bytes(content)
    return response


def parse(response):
    sel = Selector(text=response, type='html')
    blocks = sel.xpath("//div[@class='mainBox_5w']\
        /div[@class='left_w1']")
    base_url = 'http://xszz.chsi.com.cn'
    result_list = []
    for b in blocks:
        urls = b.xpath(".//table/tr/td/a/@href").extract()
        urls = [base_url+url for url in urls]
        name = b.xpath(".//table/tr/td/a/text()").extract()
        title = b.xpath("./h1/text()").extract()[0]
        name = [title+'_'+i+'.xls' for i in name]
        result_list.extend(zip(name, urls))
    return result_list

def callbackfunc(blocknum, blocksize, totalsize):
    '''回调函数
    @blocknum: 已经下载的数据块
    @blocksize: 数据块的大小
    @totalsize: 远程文件的大小
    '''
    percent = 100.0 * blocknum * blocksize / totalsize
    if percent > 100:
        percent = 100
    print "%.2f%%"% percent

def download_xls(url_list):
    """
    url_list = [('文件名','文件url')]
    """
    os.chdir("/home/dyh/data/jiangxuejin")
    for i in url_list:
        urllib.urlretrieve(i[1], i[0], callbackfunc)

url = 'http://xszz.chsi.com.cn/gjjxj/'
url_list = parse(get_response(url))
download_xls(url_list)
