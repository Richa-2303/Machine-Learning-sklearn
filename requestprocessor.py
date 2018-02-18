# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 19:09:53 2018

@author: richa
"""



import cx_Oracle as db
import datetime as dt
import csv
from collections import OrderedDict
import pickle as pk
import pandas as pd
import os



def compile_data(dbfile,tickerList):
    finalfilename="securitysharepricedetails_"+dt.date.today()+".csv"
    main_df=pd.read_csv('ml_project/{}.csv'.format(dbfile))   
    for ticker in tickerList:
        df=pd.read_csv('ml_project/{}.csv'.format(ticker))
        df.set_index('Date',inplace=True)
        df.drop(['Open','Close','High','Low','Volume'],1,inplace=True)
        df.insert(3,'Ticker',ticker,inplace=True)
        main_df=pd.merge(main_df,df, how='left',left_on='Xref_ticker,effective_date',right_on='Ticker','Date')
    
    main_df.to_csv(finalfilename)
    
    return finalfilename

def get_data_from_yahoo(tickerList,start,end):
    
           
    for ticker in tickerList:
        if not os.path.exists('ml_project/{}.csv'.format(ticker)):
            df=web.DataReader(ticker,'yahoo',start,end)
            df.to_csv('ml_project/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))
            
     

def requestprocessor(dbdetails, fetaures, tickerList , filtercond, start ,end):
    dbfile='dbfile'+dt.date.today()+'.csv'
    dict1={}
    dict2={}
    start=datetime.strptime(start,'%Y%m%d')
    end=datetime.strptime(end,'%Y%m%d')
    conn=db.connect(dbdetails)
    yahoofxdata=get_data_from_yahoo(tickerList,start,end)
    
    if not os.path.exists('ml_project'):
        os.mkdir('ml_project')
    
    
    
    cur=conn.cursor()
    table=''
    if filtercond is None:
        filtercond=1=1
        
    query='select '+ fetaures + output +' from '+ table + ' where ' +filtercond
    
    cur.execute(query)
    with open(dbfile,"wb") as dbfile:
        for row in cur:
            dbfile.write(row)
    cur.close();
    dbfile.close(); 
    
    get_data_from_yahoo(tickerList,start,end)
    finalfilename=compile_data(dbfile,tickerList) 
    
def 
        
    
