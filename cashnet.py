import Quandl
import pandas as pd
import csv
import time
import math
import numpy as np
import urllib2
import re
import os

t1=time.time()

token='XXXXXX'
yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='

companies=pd.read_csv("http://www.sharadar.com/meta/tickers.txt",sep='\t')
companies=companies[['Ticker','Name','Industry']]

print "Collecting Price to Book Data"
tickers=list(companies['Ticker'])

#yahoo api only permits 200 tickers at a time, splitting symbols accordingly
n_splits=math.ceil(len(tickers)/200.0)                
symbol_lists=np.array_split(tickers,n_splits)

pb_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=p6")
   html = response.read()
   sub_list=html.strip().split('\n')
   pb_list.append(sub_list)

pb=sum(pb_list,[])
pb=[e.strip() for e in pb]

pb_float=[]
for e in pb:
    if e=='N/A':
        pb_float.append(e)
    else:
        e=float(e)
        pb_float.append(e)

companies['P/B']=pb_float

companies=companies[(companies['P/B']<1)]

currats=[]
totliabs=[]
qpass=[]

i=0
print "Collecting Quandl Data"

for e in companies['Ticker']:
	try:
		curr=Quandl.get("SF1/"+e+"_CASHNEQ_MRQ",authtoken=token).tail(1)
		currats.append(curr.values[0][0])
		liabs=Quandl.get("SF1/"+e+"_LIABILITIES_MRQ",authtoken=token).tail(1)
		totliabs.append(liabs.values[0][0])
	except:
		qpass.append(e)
		currats.append(0)
		totliabs.append(0)
		pass
	os.system('clear')
        i+=1
        print "Progress of Quandl Collection"
        percent=(i/float(len(companies['Ticker'])))*100
        percent_format='%.2f' %percent
        print ('#'*int(percent))+' '+percent_format+'%'
        t2=time.time()
        print t2-t1
os.system('clear')
print('#'*100)+' 100%'
t2=time.time()
print t2-t1
print "Quandl Collection Done"
companies['Cash']=currats
companies['Liabilities']=totliabs


companies=companies[((companies['Cash']-companies['Liabilities'])>0)]

tickers=list(companies['Ticker'])

#yahoo api only permits 200 tickers at a time, splitting symbols accordingly
n_splits=math.ceil(len(tickers)/200.0)                
symbol_lists=np.array_split(tickers,n_splits)

cap_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=j1")
   html = response.read()
   sub_list=html.strip().split('\n')
   cap_list.append(sub_list)
 
   
market_cap=sum(cap_list,[])
market_cap=[e.strip() for e in market_cap]

def conv_to_float(x):
    if x=='N/A':
        value='N/A'
    else:
         a=x.split('.')
         all_match=[]
         for e in a:
            matches=re.findall(r'[0-9]*?.[0-9]*',e)
            all_match.append(matches)
            elem_list=sum(all_match,[])
            if len(elem_list)==3:
                num=float(elem_list[0]+'.'+elem_list[1])
            else:
                num=float(elem_list[0])
            if elem_list[-1]=='B':
                value=num*1e9
            elif elem_list[-1]=='M':
                value=num*1e6
            else:
                value=num*1e3
   
    return value

market_cap_float=[conv_to_float(e) for e in market_cap]
companies['Market_Cap']=market_cap_float
companies['Net']=companies['Cash']-companies['Liabilities']
companies['Net_Ratio']=companies['Net']/companies['Market_Cap']
companies=companies.sort(columns=['Net_Ratio','P/B'],ascending=[0,1])




companies.to_csv("cashnet.csv",index=False)
t2=time.time()
print "Done"
print qpass
print companies.shape
print t2-t1
