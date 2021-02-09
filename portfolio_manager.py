import numpy as np
import pandas as pd
import datetime
from pandas.io.data import Options
import urllib2
import time
import math

t1=time.time()
options=pd.read_csv("options.csv")
tickers=options['Symbol']
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
    frame['Percent Change']=((frame['Breakeven']-frame['Underlying_Price'])/frame['Underlying_Price'])*100

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
final_frame=final_frame[(final_frame["Buy"]>0)]
final_frame=final_frame.sort(['Percent Change','Ratio'],ascending=[1,0])
final_frame.to_csv("allocation.csv")
t2=time.time()
print final_frame.shape
print t2-t1
