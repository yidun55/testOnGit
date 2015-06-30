#!usr/bin/env python
#encoding=utf-8

  
import urllib  
import urllib2  
  
def post(url, data):  
    req = urllib2.Request(url)  
    data = urllib.urlencode(data)  
    #enable cookie  
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())  
    response = opener.open(req, data)  
    return response.read()  
  
def main():  
    posturl = "http://www.creditchina.gov.cn/search#kw='.'&page=1"  
    data = {} 
    data['areas'] = ['北京市','上海市'] 
    data['sources'] = ['安全监管总局']
    data['filterStartTime'] = ''
    data['filterEndTime'] = ''
    data['object'] = 2
    print post(posturl, data)  

if __name__ == '__main__':  
    main()  
    