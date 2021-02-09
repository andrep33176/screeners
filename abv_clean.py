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

token='XXXXXXXXXXXXXXXXXXX'
yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='

companies=pd.read_csv("http://www.sharadar.com/meta/tickers.txt",sep='\t')
companies=companies[['Ticker','Name','Industry']]
print companies.shape

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
print companies.shape

assets=[]
totliabs=[]
intangibles=[]
receivables=[]
inventories=[]
currats=[]
retearn=[]
otherinc=[]


i=0
print "Collecting Quandl Data"

for e in companies['Ticker']:
	try:
		ret=Quandl.get("SF1/"+e+"_RETEARN_MRQ",authtoken=token).tail(1)
		retearn.append(ret.values[0][0])
	except:
		retearn.append(0)
	try:
		oinc=Quandl.get("SF1/"+e+"_ACCOCI_MRQ",authtoken=token).tail(1)
		otherinc.append(oinc.values[0][0])
	except:
		otherinc.append(0)
	try:
		currasset=Quandl.get("SF1/"+e+"_ASSETSC_MRQ",authtoken=token).tail(1)
		currats.append(currasset.values[0][0])
	except:
		currats.append(0)

	try:
		asset=Quandl.get("SF1/"+e+"_ASSETS_MRQ",authtoken=token).tail(1)
		assets.append(asset.values[0][0])
	except:
		assets.append(0)
	try:	
		intangible=Quandl.get("SF1/"+e+"_INTANGIBLES_MRQ",authtoken=token).tail(1)
		intangibles.append(intangible.values[0][0])
	except:
		intangibles.append(0)
	try:	
		receivable=Quandl.get("SF1/"+e+"_RECEIVABLES_MRQ",authtoken=token).tail(1)
		receivables.append(receivable.values[0][0])
	except:
		receivables.append(0)
	try:
		inventory=Quandl.get("SF1/"+e+"_INVENTORY_MRQ",authtoken=token).tail(1)
		inventories.append(inventory.values[0][0])
	except:
		inventories.append(0)
	try:
		liabs=Quandl.get("SF1/"+e+"_LIABILITIES_MRQ",authtoken=token).tail(1)
		totliabs.append(liabs.values[0][0])
	except:
		totliabs.append(0)
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
companies['Assets']=assets
companies['Receivables']=receivables
companies['Inventory']=inventories
companies['Intangibles']=intangibles
companies['Liabilities']=totliabs
companies['Current_Assets']=currats
companies['Retained_Earnings']=retearn
companies['Other_Income']=otherinc
companies['Proxy_Prop']=companies['Assets']-companies['Current_Assets']-companies['Intangibles']-companies['Retained_Earnings']-companies['Other_Income']
companies['ABV']=companies['Assets']-companies['Liabilities']-(0.15*companies['Receivables'])-companies['Intangibles']-(0.4*companies['Inventory'])-(0.5*companies['Proxy_Prop'])




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

market_cap_num=[conv_to_float(e) for e in market_cap]
companies['Market_Cap']=market_cap
companies['Market_Cap_Num']=market_cap_num
companies['ABV_Ratio']=companies['ABV']/companies['Market_Cap_Num']
companies=companies[(companies['ABV_Ratio']>1)]
companies=companies.sort(['ABV_Ratio','P/B'],ascending=[0,1])
companies=companies[['Ticker','Name','Industry','P/B','Market_Cap','ABV_Ratio']]




companies.to_csv("abv.csv",index=False)
t2=time.time()
print "Done"
print companies.shape
print t2-t1
