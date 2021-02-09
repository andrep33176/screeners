import Quandl
import pandas as pd
import os
from ggplot import *
import time

t1=time.time()

token="XXXXXXXX"

companies=pd.read_csv("http://www.sharadar.com/meta/tickers.txt",sep="\t")
company_ticks=list(companies['Ticker'])
fs=pd.read_csv("first_tier.csv")
sa=pd.read_csv("stock_allocation.csv")
allocation=[list(fs['Ticker']),list(sa['Ticker'])]
allocation=sum(allocation,[])
allocation_ticks=list(set(allocation))
#allocation=pd.read_csv("leaps.csv")
#allocation_ticks=list(allocation['Root'])
#:allocation=pd.read_csv('allocation.csv')
#allocation_ticks=list(allocation['Ticker'])
tickers= [e for e in allocation_ticks if e in company_ticks]
not_found=[e for e in allocation_ticks if e not in company_ticks]

path="/home/andre/equities/graphs"

if not os.path.exists(path):
    os.mkdir(path)


for e in tickers:
    dataset='Premium'
    equity=Quandl.get("SF1/"+e+"_EQUITY_MRQ",authtoken=token)
    roe=Quandl.get("SF1/"+e+"_ROE_MRT",authtoken=token)
    ncfo=Quandl.get("SF1/"+e+"_NCFO_MRY",authtoken=token)
    de=Quandl.get("SF1/"+e+"_DE_MRQ",authtoken=token)
	
      		          
    first_join=pd.merge(equity,roe,left_index=True,right_index=True,how='outer')
    second_join=pd.merge(first_join,ncfo,left_index=True,right_index=True,how='outer')
    final_join=pd.merge(second_join,de,left_index=True,right_index=True,how='outer')
    final_join['Date']=list(final_join.index)
    final_join.columns=['Equity','ROE','NCFO','DE','Date']
    final_join=final_join.tail(15)
    equity_plot=ggplot(aes(x='Date',y='Equity'),final_join)+geom_point()+geom_line()+xlab(e+" "+dataset)
    roe_plot=ggplot(aes(x='Date',y='ROE'),final_join)+geom_point()+geom_line()+xlab(e+" "+dataset)+\
    geom_hline(yintercept=[final_join['ROE'].mean()])
    cash_flow_plot=ggplot(aes(x='Date',y='NCFO'),final_join)+geom_point()+geom_line()+xlab(e+" "+dataset)+\
    geom_hline(yintercept=[final_join['NCFO'].mean()])
    deb2plot=ggplot(aes(x='Date',y='DE'),final_join)+geom_point()+geom_line()+xlab(e+" "+dataset)
    ggsave(equity_plot,"graphs/"+e+" Equity.png")
    ggsave(roe_plot,"graphs/"+e+" ROE.png")
    ggsave(cash_flow_plot,"graphs/"+e+" Cash_Flow.png")
    ggsave(deb2plot,"graphs/"+e+" Deb2E.png")
    
t2=time.time()
print not_found
print t2-t1

