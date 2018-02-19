# -*- coding: utf-8 -*-
"""
Created on Sat Feb 10 19:09:53 2018

@author: richa
"""



import cx_Oracle as db
import datetime as dt
import os



def csv_to_numpy_train(fileLocation):
    
    
    from numpy import genfromtxt
    my_data = genfromtxt(fileLocation, delimiter=',')
           
    features=my_data[:,:-1]
    target=my_data[:,-1:]
    return features,target
    
def csv_to_numpy_pred(fileLocation):
  
    from numpy import genfromtxt
    features = genfromtxt(fileLocation, delimiter=',')
           
    return features  
    
    
def db_to_csv(dbdetails, fetaures, isinList , filtercond, start ,end):
    dbfile='dbfile'+dt.date.today()+'.csv'
    conn=db.connect(dbdetails)
    
    if not os.path.exists('ml_project'):
        os.mkdir('ml_project')
    
    
    
    cur=conn.cursor()
    table=''
    if filtercond is None:
        filtercond=''
        
        
        
    query='select '+ fetaures + output +' from '+ table + ' where ' +filtercond
    
    cur.execute(query)
    with open(dbfile,"wb") as dbfile:
        for row in cur:
            dbfile.write(row)
    cur.close();
    dbfile.close(); 
    
def buyselltrainer():
    
    feature,target=csv_to_numpy_train()
    
    
    from ClassifyDT import classify
    clf=classify()
    from sklearn.cross_validation import train_test_split
    feature_train,feature_test,target_train,target_test=train_test_split(feature,target,test_size=0.5,random_state=42)
    clf.fit(feature_train,target_train)
    pred=clf.predict(feature_test)
    from sklearn.metrics import accuracy_score
    accuracy=accuracy_score(target_test,pred)
    print(accuracy)
    return clf
    
def buysellpredicter(fileLocation):   
    
    clf=buyselltrainer()
    features=csv_to_numpy_pred(fileLocation)
    pred=clf.predict(features)
    print(pred)

def requestProcessor(dbdetails, fetaures, isinList , filtercond, start ,end,fileLocation):
    
    db_to_csv(dbdetails, fetaures, isinList , filtercond, start ,end)
    buyselltrainer()
    buysellpredicter(fileLocation)
    

    
    
        
    
