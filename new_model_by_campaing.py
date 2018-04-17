# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 13:12:06 2018

@author: L3_OOgunniya
"""

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import scipy
import pandas.io.sql
import pyodbc
from scipy.stats import binom, hypergeom

import math as mt
from sklearn.linear_model import LogisticRegression
from sklearn import preprocessing

conn1 = pyodbc.connect(
    r'DRIVER={ODBC Driver 13 for SQL Server};'
    r'SERVER=GB-UXS-WBIDV-D1;'
    r'DATABASE=PBRDM;'
    r'Trusted_Connection=yes;'
    )
dfall= pandas.io.sql.read_sql("""
                          
select cr.EmailAddress ,cast(convert(varchar(10),cc.SentDate,110) as date )'SentDate',cr.CampaignId,co.EmailAddress'EmailAddressOpen',cast(convert(varchar(10),co.[Date],110) as date)'DateOpen',co.CampaignId'CampaignIdOpen',wgc.Gender, wgc.BirthDate
from [PBRDMStaging].[dbo].[CampaignMonitor_ClientCampaign] cc \
inner join [PBRDMStaging].[dbo].[CampaignMonitor_CampaignRecipient] cr  on cr.CampaignID = cc.CampaignId \
left join [PBRDMStaging].[dbo].[CampaignMonitor_CampaignOpen] co on cr.CampaignId = co.CampaignId  and cr.EmailAddress = co.EmailAddress \
inner join [PBRDM].[dbo].[W_GoldenConsumer] wgc on  wgc.Email = cr.EmailAddress \
where cr.CampaignId in ( '24edd89631d2a59b456dac69df1227e2')---,
							---'f05e932da8430c3c4abca623ad36efd9',
						---	'0224df673ec4295a5129b329b2d08826',
						---	'73dc2f7c5a26197b9a726a21ad3e0702',
							---'6c442b3096a8bad9d76cf009f16efa3d',
							---'f6efcceffe0740768ba873a5583fd576',
							---'c7cd17d9eebc23577d63e90ef9f3f42a',
							---'3ade2e32978ff813b985d6ac3bb2529f',
							---'264645dd16bed1ee4aa37c07efd87439',
							---'63284bcd98e48f9332386fb5a1393d03')
 and (wgc.Gender is not Null and wgc.Gender <> ' ') and (wgc.BirthDate is not Null and wgc.BirthDate <> ' ')
                              
""", conn1)

conn1.commit()
conn1.close()


dfall=dfall.drop_duplicates(['EmailAddress','CampaignId','SentDate'],keep='first')
dfall['CampaignIdOpen']=dfall['CampaignIdOpen'].fillna(0)


def pivot_index(x):
    listo=[]
    
    y=x.tolist()

    for i in range(len(y)):
        if y[i] == 0:
            status = -1
        else:
            status =1
        listo.append(status)
    return listo

def dateformat(x):
    x=pd.to_datetime(x)
    return x


def date_check(x,y):
    listo=[]
    for i in range(len(x)):
        try:
            difference = (y[i] -x[i]).days
        except ValueError:
            difference = 0
        listo.append(difference)
    return listo

def cleandays(x):
    listo=[]
    for i in x:
        if i>= 0:
            status=i+1
        else:
            status =0
        listo.append(status)
    return listo

def Sex(x):
    y=x.tolist()
    listo=[]
    for i in y:
        if i == 'Male' or i == 'male' or i =='M':
            sx= 1
        elif i == 'Female' or i == 'female' or i =='f':
            sx=0
        listo.append(sx)
    return listo

def Age(today,x):
        
   agin = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
   return agin
    

def dateformat2(x):
    stringformat = '%a %b %d %Y %H:%M:%S GMT+0000 (UTC)'
    today = dt.datetime.now().date()
    listo=[]
    for i in x:
      
                    
            if len(str(i)) > 24:
                 ts = str(i).split()      
                 New_Date =  dt.datetime.strptime(' '.join(ts), stringformat).date()
                 myage = Age(today,New_Date) 
            elif len(str(i)) > 10 and len(str(i)) < 24:
                 New_Date = dt.datetime.strptime(str(i), '%Y-%m-%d %H:%M:%S').date()
                 myage = Age(today,New_Date) 
            elif len(str(i)) >= 24 and len(str(i)) < 28:
                 New_Date = dt.datetime.strptime(str(i), '%Y-%m-%dT%H:%M:%S.%fZ').date()
                 myage = Age(today,New_Date) 
            else:
                 New_Date = pd.to_datetime(str(i)).date()
                 myage = Age(today, New_Date)
    
                 
            listo.append(myage)
        
    return listo

def Target(x):
    if x != 0:
        status= 1
    else:
        status = 0
    
    return status

def Engagement(x):
    if x <= 0.0001:
        status ='No Engagement'
    elif x >0.0001 and x <= 0.25:
        status = 'Low Engagement'
    elif x >0.25 and x<=0.5:
        status = 'Medium Engagement'
    elif x>0.5 and x<= 0.89:
        status = 'High Engagement'
    elif x > 0.89:
        status = 'Very High Engagement'
    return status


dfall=dfall[~dfall.BirthDate.str.startswith('0')]
dfall['Age']= dateformat2(dfall.BirthDate.tolist())
dfall['Sex']= Sex(dfall.Gender)
dfall['SentDate'] = dateformat(dfall['SentDate'])
dfall['DateOpen'] = dateformat(dfall['DateOpen'])
dfall['Difference'] = date_check(dfall['SentDate'].tolist(),dfall['DateOpen'].tolist())
dfall['Difference'] = cleandays(dfall['Difference'].tolist())
dfall['Target'] = dfall.CampaignIdOpen.apply(Target)
df=dfall[['EmailAddress','Age','Sex','Difference','Target']]
df = df[(df.Age > 0) & (df.Age < 100) ]

df2 = df.columns.get_values()
names = df2.tolist()
ind2remove = ['EmailAddress']
names = [i for i in names if i not in ind2remove]

propensity = LogisticRegression()
propensity = propensity.fit(df[names[0:-1]], df.Target)
output=propensity.predict_proba(df[names[0:-1]])
pscore = output[:,1] # The predicted propensities by the model
df['Propensity']=pscore
df['Engaged?'] = df.Propensity.apply(Engagement)

summary0= df.groupby(['Sex'])['Propensity'].count()

summary1= df.groupby(['Engaged?'])['Propensity','Difference'].mean()
summary12= df.groupby(['Engaged?'])['Propensity'].count()
summary2= df.groupby(['Engaged?','Sex'])['Propensity','Difference'].mean()
summary21= df.groupby(['Engaged?','Sex'])['Propensity'].count()
summary3= df.groupby(['Engaged?','Age'])['Propensity','Difference'].mean()
summary31= df.groupby(['Engaged?','Age'])['Propensity'].count()


#resultgroup= df.groupby(['Engaged?','Sex','Age'])['Propensity','Difference'].mean()
#resultgroup1= df.groupby(['Engaged?','Sex','Age'])['Propensity'].count()
#plt.hist(df['Propensity'])
#plt.show()