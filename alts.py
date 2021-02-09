import pandas as pd
import numpy as np
import math
from BeautifulSoup import BeautifulSoup
import re
import urllib2
from StringIO import StringIO
import time




t1=time.time()

stocks=pd.read_csv('http://www.otcmarkets.com/reports/symbol_info.csv?__hstc=139045612.f8b461f9bbbfabe305d395d10d6dac4d.1357141537636.1373314633650.1373574135734.89&__hssc=139045612.4.1373574135734')

stocks=stocks[(stocks['Caveat Emptor']=='N')]
stocks=stocks[(stocks['Country of Domicile']=='USA')|(stocks['Country of Domicile']=='Canada')]
stocks=stocks[(stocks['OTC Tier'].str.contains('OTCQX|OTCQB'))]
stocks=stocks[(stocks['Security Type'].str.contains('Rights|Warrants'))]
stocks=stocks[['Symbol','Security Name']]
print stocks.shape
url="http://www.nasdaq.com/screening/companies-by-industry.aspx?\
industry=ALL&region=North+America&country=United+States&render=download"



def fetch_file(url):

    file_fetcher = urllib2.build_opener()
    file_fetcher.addheaders = [('User-agent', 'Mozilla/5.0')]
    file_data = file_fetcher.open(url).read()
    file_data=file_data[:-1]
    file_data=StringIO(file_data)
    file_data=pd.read_csv(file_data,sep=",")
    return file_data

frame=fetch_file(url)

nasurl="ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"

nasdaq=pd.read_csv(nasurl,sep ="|")
nasdaq=nasdaq[nasdaq["Security Name"].str.contains("Rights|Warrants",na=True)]
nasdaq=nasdaq[['Symbol','Security Name']]
print nasdaq.shape

otherurl="ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
other=pd.read_csv(otherurl,sep ="|")
other=other[other["Security Name"].str.contains("Rights|Warrants",na=True)]
other.columns=['Symbol','Security Name','Exchange','CQS Symbol','ETF','Round Lot Size','Test Issue','NASDAQ Symbol']
other=other[['Symbol','Security Name']]
print other.shape
stocks=stocks.append(nasdaq)
stocks=stocks.append(other)
print stocks.shape

stocks.to_csv('alts.csv',index=False)
t2=time.time()
print t2-t1
print "Done"

