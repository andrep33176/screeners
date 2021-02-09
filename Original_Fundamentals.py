import pandas.io.html as ph
import datetime
import csv
import urllib2
import pandas as pd
import numpy as np
import math
from BeautifulSoup import BeautifulSoup
import re
import time
import math


t1=time.time()
inflation_rate=0.03
bank_roll=XXXXX
token='XXXXXXXXX'

#figure days to expiry
expiry=datetime.date(2016,1,16)
today=datetime.date.today()
time_left=expiry-today
#fetch Leap Data from cboe
url = 'http://www.cboe.com/publish/ScheduledTask/MktData/cboesymboldir2.csv'
response = urllib2.urlopen(url)
#Parse csv into pandas readable format
cr = csv.reader(response)
rows=[]
for row in cr:
    rows.append(row)
rel_rows=rows[2:]

#Create Dataframe
names=["Name","Symbol","DPM Name","Cycle","Traded at C2","Leaps 2015","Leaps 2016","Leaps 2017","Product Type","10","11","12","13"]
options=pd.DataFrame(rel_rows)
options.columns=names
#Cull Possible Leaps
rel_variables=options[['Name','Symbol','DPM Name','Leaps 2016','Leaps 2017','Product Type','10','11','12','13']]
rel_options=rel_variables[(rel_variables['Product Type']=='L')|(rel_variables['10']=='L')|(rel_variables['11']=='L')|(rel_variables['12']=='L')\
|(rel_variables['Leaps 2016']=='Y')|(rel_variables['Leaps 2017']=='Y')|(rel_variables['13']=='L')]
ticker_symbols=(rel_options['Symbol'])

yahoo_cap_url='http://finance.yahoo.com/d/quotes.csv?s='

#yahoo api only permits 200 tickers at a time, splitting symbols accordingly
n_splits=math.ceil(len(ticker_symbols)/200.0)                
symbol_lists=np.array_split(ticker_symbols,n_splits)   

#fetching and concatenating market cap information
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


market_cap=[e.strip() for e in market_cap] 
#Converting market cap information into a searchable floating point number
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
  

def key_stat(SYM):
    url = "http://finance.yahoo.com/q/ks?s=" + SYM + "+Key+Statistics"
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page)
    res = [[x.text for x in y.parent.contents] for  y in soup.findAll('td', attrs={"class" : "yfnc_tablehead1"})]
    res=sum(res,[])
    res=[str(e) for e in res]
    res=dict(zip(res[::2],res[1::2]))
    return res

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

market_cap_float=[conv_to_float(e) for e in market_cap]
rel_options['Market_Cap']=market_cap
rel_options['Market_Cap_Num']=market_cap_float
rel_options['P/B']=pb_float
rel_options['P/E']=pe_float
rel_options['P/S']=ps_float
rel_options['EPS']=eps_float
rel_options['Price']=price
rel_options['PEG']=peg_float
rel_options['High']=high



small_cap=rel_options.sort(columns=['P/B','P/E','P/S','EPS'],ascending=[1,1,1,0])
rel_small=small_cap[(small_cap['EPS']>0)&(small_cap['P/E']<15)&(small_cap['P/B']<=1.5)]
rel_small=rel_small[['Name','Symbol','Market_Cap','Market_Cap_Num','P/B','P/E','P/S','EPS','PEG','Price','High']]

symbols=(rel_small['Symbol'])


cfs=[]
lcfs=[]
deb2es=[]
cash=[]

for e in symbols:
	try:
		data=key_stat(e)
    		cf=data['Operating Cash Flow (ttm):']
    		lcf=data['Levered Free Cash Flow (ttm):']
		cps=data['Total Cash Per Share (mrq):']
    		deb2e=data['Total Debt/Equity (mrq):']
    		deb2e=deb2e.replace(",", "")
		cps=cps.replace(",","")
    		cfs.append(cf)
    		lcfs.append(lcf)
    		deb2es.append(deb2e)
		cash.append(cps)
	except:
		cfs.append('N/A')
		lcfs.append('N/A')
		deb2es.append('N/A')
		cash.append('N/A')
	
cash=[e if e!='N/A' else 0 for e in cash]
cash_float=[float(e) for e in cash]
cfs_float=[conv_to_float(e) for e in cfs]
cfs_float=[e if e!='N/A' else 1 for e in cfs_float]
lcfs_float=[conv_to_float(e) for e in lcfs]
lcfs_float=[e if e!='N/A' else 1 for e in lcfs_float]
deb2es_float=[]
for e in deb2es:
    if e=='N/A':
        deb2es_float.append(e)
    else:
        e=float(e)
        deb2es_float.append(e)
        
rel_small['CPS']=cash_float
rel_small['Cash_Flow']=cfs
rel_small['Cash_Flow_Num']=cfs_float
rel_small['Levered_Cash_Flow']=lcfs
rel_small['Levered_Cash_Flow_Num']=lcfs_float
rel_small['Debt_to_Equity']=deb2es_float

#rel_small=rel_small[(rel_small['Levered_Cash_Flow_Num']>0)&(rel_small['Cash_Flow_Num']>0)]

def dcf(cash_flow):
    if cash_flow =='N/A':
        return 'N/A'
    else:
        cash=np.ones((10))*cash_flow
        vec= np.ones((10))-(np.arange(1,11)*inflation_rate)
        return sum(vec*cash)


dcfs=[dcf(e) for e in rel_small['Cash_Flow_Num']]
rel_small['DCF']=dcfs
rel_small['DCF']=rel_small['DCF'].replace('N/A',0)
rel_small['Debt_to_Equity']=rel_small['Debt_to_Equity'].replace('N/A',0)
rel_small['DCF_Val']=rel_small['DCF']/rel_small['Market_Cap_Num']
rel_small['Profit_Margin']=(rel_small['P/S']/rel_small['P/E'])*100
rel_small['ROE']=(rel_small['P/B']/rel_small['P/E'])*100
rel_small['Debt_to_Earnings']=rel_small['Debt_to_Equity']/rel_small['ROE']
rel_small['GV']=rel_small['P/E']*rel_small['P/B']
rel_small=rel_small[(rel_small['Debt_to_Equity']<55)|(rel_small['Debt_to_Earnings']<5.1)]
rel_small['GN']=(rel_small['EPS']*(rel_small['Price']/rel_small['P/B'])*22.5)**0.5
small_cap=rel_small.sort(columns=['DCF_Val','ROE','Profit_Margin','Debt_to_Earnings'],ascending=[0,0,0,1])

rel_var=small_cap[['Name','Symbol','Market_Cap','DCF','DCF_Val','ROE','Debt_to_Equity','Debt_to_Earnings',\
		'P/B','P/E','GV','P/S','EPS','Profit_Margin','PEG',\
		'Cash_Flow','Cash_Flow_Num','Levered_Cash_Flow','CPS','Price','High','GN']]
#rel_var=rel_var[(rel_var['DCF_Val']>1)|(rel_var['Cash_Flow_Num']==1)]




print rel_var.shape

rel_var.to_csv('options.csv',index=False)
t2=time.time()
print t2-t1

