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

token='XXXXX'
yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='

companies=pd.read_csv("http://www.sharadar.com/meta/tickers.txt",sep='\t')
companies=companies[['Ticker','Name','Industry']]
ncfo_avgs=[]
ncfo_stds=[]
ncfo_skews=[]
capex_avgs=[]
capex_stds=[]
debs=[]
ponepass=[]
i=0
print "Starting Part 1:"
for e in companies['Ticker']:
    try:
        de=Quandl.get("SF1/"+e+"_DE_MRQ",authtoken=token).tail(1)
        debs.append(de.values[0][0])
    except:
	    ponepass.append(e)
	    debs.append(100)
	    pass
    os.system('clear')
    i+=1
    print "Progress of Part 1"
    percent=(i/float(len(companies['Ticker'])))*100
    percent_format='%.2f' %percent
    print ('#'*int(percent))+' '+percent_format+'%'
    t2=time.time()
    print t2-t1
os.system('clear')
print('#'*100)+' 100%'
t2=time.time()
print t2-t1
print "Part 1 Done"

ptwopass=[]
companies['Debt/Eq']=debs
companies=companies[(companies['Debt/Eq']<0.55)&(companies['Debt/Eq']>0)]
i=0
print "Starting Part 2"

for e in companies['Ticker']:
    try:	
    	ncfo=Quandl.get("SF1/"+e+"_NCFO_MRY",authtoken=token).tail()
    	ncfo_avgs.append(float(ncfo.mean()))
    	ncfo_stds.append(float((ncfo.std()/ncfo.mean())*100))
    	ncfo_skews.append(float(ncfo.skew()))
    	capex=Quandl.get("SF1/"+e+"_CAPEX_MRY",authtoken=token).tail()
    	capex_avgs.append(float(capex.mean()))
    	capex_stds.append(float((capex.std()/capex.mean())*100))
    except:
	    ptwopass.append(e)
	    ncfo_avgs.append(0)
	    ncfo_stds.append(0)
	    ncfo_skews.append(0)
	    capex_avgs.append(0)
	    capex_stds.append(0)
	    pass
    os.system('clear')
    print "Progress of Part 2:"
    i+=1
    percent=(i/float(len(companies['Ticker'])))*100
    percent_format='%.2f' %percent
    print ('#'*int(percent))+' '+percent_format+'%'
    t2=time.time()
    print t2-t1
os.system('clear')
print('#'*100)+' 100%'
t2=time.time()
print t2-t1
print "Done"
companies['NCFO']=ncfo_avgs
companies['CFODev']=ncfo_stds
companies['Skew']=ncfo_skews
companies['Capex']=capex_avgs
companies['CapexDev']=capex_stds 
companies['CapexPerc']=(companies['Capex']/companies['NCFO'])*100

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
companies['DCF']=((0.8*companies['NCFO']*10))/1.5
companies=companies[(companies['DCF']>companies['Market_Cap'])]
companies['Projected_Gain']=((companies['DCF']/companies['Market_Cap'])-1)*100
companies=companies[(companies['Projected_Gain']>15)]
companies=companies.sort(columns=['CFODev','CapexPerc','Projected_Gain'],ascending=[1,1,0])

t2=time.time()
companies.to_csv('aesop.csv',index=False)
print "Program finished"
print ponepass
print ptwopass
print companies.shape
print t2-t1




