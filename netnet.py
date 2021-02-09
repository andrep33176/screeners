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

token='XXXXXXXXX'
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
price_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=l1")
   html = response.read()
   sub_list=html.strip().split('\n')
   price_list.append(sub_list)
 
   
price=sum(price_list,[])
price=[e.strip() for e in price]
price=[float(e) if e!='N/A' else 0 for e in price]

high_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=k")
   html = response.read()
   sub_list=html.strip().split('\n')
   high_list.append(sub_list)
 
   
high=sum(high_list,[])
high=[e.strip() for e in high]

yield_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=y")
   html = response.read()
   sub_list=html.strip().split('\n')
   yield_list.append(sub_list)
 
   
yield_list=sum(yield_list,[])
yield_list=[e.strip() for e in yield_list]

pe_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=r")
   html = response.read()
   sub_list=html.strip().split('\n')
   pe_list.append(sub_list)

pe=sum(pe_list,[])
pe=[e.strip() for e in pe]

peg_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=r5")
   html = response.read()
   sub_list=html.strip().split('\n')
   peg_list.append(sub_list)

peg=sum(peg_list,[])
peg=[e.strip() for e in peg]
peg_float=[]
for e in peg:
    if e=='N/A':
        pass
    else:
        e=float(e)
    peg_float.append(e)

eps_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=e")
   html = response.read()
   sub_list=html.strip().split('\n')
   eps_list.append(sub_list)

eps=sum(eps_list,[])
eps=[e.strip() for e in eps]
eps_float=[]
for e in eps:
    if e=='N/A':
        pass
    else:
        e=float(e)
    eps_float.append(e)


pb_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=p6")
   html = response.read()
   sub_list=html.strip().split('\n')
   pb_list.append(sub_list)

pb=sum(pb_list,[])
pb=[e.strip() for e in pb]


ps_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=p5")
   html = response.read()
   sub_list=html.strip().split('\n')
   ps_list.append(sub_list)

ps=sum(ps_list,[])
ps=[e.strip() for e in ps]



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


pe_float=[]
for e in pe:
    if e=='N/A':
        pe_float.append(e)
    else:
        e=float(e)
        pe_float.append(e)
        
pb_float=[]
for e in pb:
    if e=='N/A':
        pb_float.append(e)
    else:
        e=float(e)
        pb_float.append(e)
        
ps_float=[]
for e in ps:
    if e=='N/A':
        ps_float.append(e)
    else:
        e=float(e)
        ps_float.append(e)



companies['P/B']=pb_float
companies['P/E']=pe_float
companies['P/S']=ps_float
companies['EPS']=eps_float
companies['Price']=price
companies['PEG']=peg_float
companies['High']=high
companies['Dividend_Yield']=yield_list

companies=companies[(companies['P/B']<1)]

currats=[]
totliabs=[]
qpass=[]

i=0
print "Collecting Quandl Data"

for e in companies['Ticker']:
	try:
		curr=Quandl.get("SF1/"+e+"_ASSETSC_MRQ",authtoken=token).tail(1)
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

companies['Current_Assets']=currats
companies['Liabilities']=totliabs


companies=companies[((companies['Current_Assets']-companies['Liabilities'])>0)]

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
companies['Net']=companies['Current_Assets']-companies['Liabilities']
companies['Net_Ratio']=companies['Net']/companies['Market_Cap']
companies=companies.sort(columns=['Net_Ratio'],ascending=[0])




companies.to_csv("netnet.csv",index=False)
t2=time.time()
print "Done"
print qpass
print companies.shape
print t2-t1
