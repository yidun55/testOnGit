#!usr/bin/env python
#encoding: utf-8

import os
import re
from scrapy import Selector
os.chdir("C:/Users/LENOVO/Desktop")

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

f = open("xpath.txt")
html = f.read()
html = bytes(html)
sel = Selector(text=html, type='html')

te = sel.xpath(u"//td[text()='您查询的手机号码段']\
    /following-sibling::td[1]/text()").extract()
print te