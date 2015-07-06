#!usr/bin/env python
#coding: utf-8
import re
import urllib
import urllib2
import cookielib
import base64
import binascii
import os
import json
import sys
import cPickle as p
import rsa
import gzip

reload(sys)
sys.setdefaultencoding('utf-8') #将字符编码置为utf-8
luckyList=[] #红包列表
lowest=10 #能忍受红包领奖记录最低为多少

cj = cookielib.CookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
urllib2.install_opener(opener)

def getData(url) :
    try:
        req  = urllib2.Request(url)
        result = opener.open(req)
        text = result.read()
        # print text
        text=text.decode("utf-8").encode("gbk",'ignore')
        # print text
        return text
    except Exception, e:
        print u'请求异常,url:'+url
        print e

def postData(url,data,header) :
    try:
        data = urllib.urlencode(data) 
        req  = urllib2.Request(url,data,header)
        result = opener.open(req)
        text = result.read()
        # text = gzipData(result.read())
        return text
    except Exception, e:
        print u'请求异常,url:'+url


def login(nick , pwd) :
    print u"----------登录中----------"
    print  "----------......----------"
    prelogin_url = 'http://login.sina.com.cn/sso/prelogin.php?entry=weibo&callback=sinaSSOController.preloginCallBack&su=%s&rsakt=mod&checkpin=1&client=ssologin.js(v1.4.15)&_=1400822309846' % nick
    preLogin = getData(prelogin_url)
    servertime = re.findall('"servertime":(.+?),' , preLogin)[0]
    pubkey = re.findall('"pubkey":"(.+?)",' , preLogin)[0]
    rsakv = re.findall('"rsakv":"(.+?)",' , preLogin)[0]
    nonce = re.findall('"nonce":"(.+?)",' , preLogin)[0]
    #print bytearray('xxxx','utf-8')
    su  = base64.b64encode(urllib.quote(nick))
    rsaPublickey= int(pubkey,16)
    key = rsa.PublicKey(rsaPublickey,65537)
    message = str(servertime) +'\t' + str(nonce) + '\n' + str(pwd)
    sp = binascii.b2a_hex(rsa.encrypt(message,key))
    header = {'User-Agent' : 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'}
    param = {
        'entry': 'weibo',
        'gateway': '1',
        'from': '',
        'savestate': '7',
        'userticket': '1',
        'ssosimplelogin': '1',
        'vsnf': '1',
        'vsnval': '',
        'su': su,
        'service': 'miniblog',
        'servertime': servertime,
        'nonce': nonce,
        'pwencode': 'rsa2',
        'sp': sp,
        'encoding': 'UTF-8',
        'url': 'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
        'returntype': 'META',
        'rsakv' : rsakv,
        }
    s = postData('http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)',param,header)
    # print dir(s)
    print s
    try:
        urll = re.findall("location.replace\(\'(.+?)\'\);" , s)[0]
        login=getData(urll)
        print u"---------登录成功！-------"
        print  "----------......----------"
    except Exception, e:
        print u"---------登录失败！-------"
        print  "----------......----------"
        exit(0)

login('heshang1203@sina.com' , '')


