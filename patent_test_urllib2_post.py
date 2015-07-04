#!usr/bin/env python
#coding: utf-8

import urllib, urllib2


url = "http://epub.sipo.gov.cn"
data = {'strWord':"申请（专利权）人,发明（设计）人,代理人+='%百度%' or 地址,名称,专利代理机构,摘要+='百度'",
        "showType":"1","numSortMethod":"4","strLicenseCode":"",
        "selected":"","pageSize":"3","pageNow":"1",
        "numFMGB":"0","numFMSQ":"0","numSYXX":"0","numWGSQ":"0"}
header = {'User-Agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36",
          'Referer':"http://epub.sipo.gov.cn"}

req = urllib2.Request(url, data=urllib.urlencode(data), headers=header)
xtml = urllib2.urlopen(req)
content = xtml.read()
print content