import pandas as pd
import numpy as np
import math
from BeautifulSoup import BeautifulSoup
import re
import urllib2
from StringIO import StringIO
import time

def conv_value(x):
	x=x.replace(',','')
        return float(x.replace('$',''))

inflation_rate=0.03
t1=time.time()

stocks=pd.read_csv('insider.csv')
stocks=stocks[['Ticker','Company']]
stocks=stocks.drop_duplicates()
print stocks.shape





tickers=list(stocks['Ticker'])
companies=list(stocks['Company'])




stocks_dict={'Ticker':tickers,'Company':companies}
stocks=pd.DataFrame(stocks_dict)


#yahoo api only permits 200 tickers at a time, splitting symbols accordingly
n_splits=math.ceil(len(tickers)/200.0)                
symbol_lists=np.array_split(tickers,n_splits)


yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='


cap_list=[]
for e in symbol_lists:
   symbol_concat='+'.join(e)
   response = urllib2.urlopen(yahoo_cap_url+symbol_concat+"&f=j1")
   html = response.read()
   sub_list=html.strip().split('\n')
   cap_list.append(sub_list)
 
   
market_cap=sum(cap_list,[])
market_cap=[e.strip() for e in market_cap]

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
        pe_float.append(0)
        
pb_float=[]
for e in pb:
    if e=='N/A':
        pb_float.append(0)
    else:
        e=float(e)
        pb_float.append(e)
        
ps_float=[]
for e in ps:
    if e=='N/A':
        ps_float.append(0)
    else:
        e=float(e)
        ps_float.append(e)

market_cap_float=[conv_to_float(e) for e in market_cap]


stocks['Market_Cap']=market_cap
stocks['Market_Cap_Num']=market_cap_float
stocks['P/B']=pb_float
stocks['P/E']=pe_float
stocks['P/S']=ps_float
stocks['EPS']=eps_float
stocks['Price']=price
stocks['PEG']=peg_float
stocks['High']=high
stocks['Dividend_Yield']=yield_list
stocks=stocks.sort(columns=['P/B','P/E','P/S','EPS'],ascending=[1,1,1,0])
stocks=stocks[['Company','Ticker','Market_Cap','Market_Cap_Num','P/B','P/E','P/S','EPS','Dividend_Yield','PEG','Price','High']]

insider=pd.read_csv('insider.csv')
insider=insider[['Ticker','Share Price','Sum','Trade Date']]

stocks=stocks.merge(insider,how='outer',on='Ticker')
stocks['Max_Price']=stocks.groupby('Ticker')['Share Price'].transform('max')
stocks['Last_Purchase']=stocks.groupby('Ticker')['Trade Date'].transform('max')
stocks=stocks.drop(['Trade Date'],axis=1)
stocks=stocks.drop(['Share Price'],axis=1)
stocks['Max_Price']=stocks['Max_Price'].apply(conv_value)
stocks['Max_Price']=stocks['Max_Price'].astype(float)
stocks=stocks.drop_duplicates()
stocks['Price']=stocks['Price'].astype(float)
stocks['Margin']=stocks['Max_Price']-stocks['Price']
stocks['Margin_Percent']=(stocks['Margin']/stocks['Price'])*100
stocks=stocks.sort_values(['Last_Purchase','Margin_Percent','Sum'],ascending=[0,0,0])

print stocks.shape

stocks.to_csv('inside_data.csv',index=False)
t2=time.time()
print t2-t1
