import numpy as np
import pandas as pd
import datetime
from pandas.io.data import Options
import urllib2
import time
import math

t1=time.time()
options=pd.read_csv("candidates.csv")
tickers=options['Ticker']
#tickers=['GNW','MBI','VLO','BP']
yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='
bank_roll=14000

offers=[]
for e in tickers:
    expiry_2=(2018,1)
    expiry_1=(2017,1)



    tick=Options(e,'yahoo')
    try:
        frame=tick.get_call_data(month=expiry_2[1],year=expiry_2[0])
        a=list(frame.index)
    except:
	try:
            frame=tick.get_call_data(month=expiry_1[1],year=expiry_1[0])
            a= list(frame.index)
	except:
		print e
		pass



    strike=[]
    for e in a:
        strike.append(e[0])
    frame['Strike']=strike
    frame['Mark']=frame["Bid"]+((frame["Ask"]-frame["Bid"])/2)
    frame['Ratio']=frame['Strike']/frame['Mark']
    #additional 0.17 in breakeven accounts for commission costs .015 cents to buy and 15 cents to exercise rounded up
    frame['Breakeven']=frame['Strike']+frame['Mark']+0.17
    frame['Percent_Change']=((frame['Breakeven']-frame['Underlying_Price'])/frame['Underlying_Price'])*100

    offers.append(frame.ix[frame['Breakeven'].idxmin()])
final_frame= pd.concat(offers,axis=1)
final_frame=final_frame.transpose()
final_frame=final_frame.drop(["Chg","Quote_Time","Underlying","IsNonstandard",\
		"PctChg",'IV'],axis=1)
high_list=[]
for e in tickers:
   symbol_concat=e
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=k")
   html = response.read()
   sub_list=html.strip().split('\n')
   high_list.append(sub_list)
 
   
high=sum(high_list,[])
high=[e.strip() for e in high]
high=[float(e) for e in high]
final_frame["High"]=high
final_frame["Odds"]=((final_frame["High"])-final_frame["Breakeven"])/final_frame["Mark"]
final_frame["Absolute_Odds"]=final_frame["Odds"].map(math.fabs)
final_frame["Kelly_Fraction"]=(((final_frame["Odds"]*0.5)-0.5)/(2*(final_frame["Absolute_Odds"])))*bank_roll
final_frame["Buy_Original"]=(final_frame["Kelly_Fraction"]/(final_frame["Mark"]*100))
buy_list= list(final_frame["Buy_Original"])
floored_list=[np.floor(e) for e in buy_list]
final_list=[]
for e in floored_list:
    if e!=0:
        final_list.append(e)
    else:
        final_list.append(1)
final_frame["Buy"]=final_list
final_frame["Final_Cost"]=final_frame["Buy"]*(final_frame["Mark"]*100)
final_frame=final_frame.drop(["Odds"],axis=1)
#final_frame=final_frame[(final_frame["Buy"]>0)]
final_frame=final_frame.sort(columns=['Percent_Change','Ratio'],ascending=[1,0])
ticker_symbols=final_frame['Root']
#yahoo api only permits 200 tickers at a time, splitting symbols accordingly
n_splits=math.ceil(len(ticker_symbols)/200.0)                
symbol_lists=np.array_split(ticker_symbols,n_splits)   

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
    
pe_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=r")
   html = response.read()
   sub_list=html.strip().split('\n')
   pe_list.append(sub_list)

pe=sum(pe_list,[])
pe=[e.strip() for e in pe]

pe_float=[]
for e in pe:
    if e=='N/A':
        pe_float.append(e)
    else:
        e=float(e)
        pe_float.append(e)
        
final_frame['P/B']=pb_float
final_frame['EPS']=eps_float
final_frame['P/E']=pe_float


final_frame.to_csv("leaps.csv")
t2=time.time()
print final_frame.shape
print t2-t1
