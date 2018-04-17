# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:13:23 2018

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
where cr.CampaignId in ( '24edd89631d2a59b456dac69df1227e2',
							'f05e932da8430c3c4abca623ad36efd9',
							'0224df673ec4295a5129b329b2d08826',
							'73dc2f7c5a26197b9a726a21ad3e0702',
							'6c442b3096a8bad9d76cf009f16efa3d',
							'f6efcceffe0740768ba873a5583fd576',
							'c7cd17d9eebc23577d63e90ef9f3f42a',
							'3ade2e32978ff813b985d6ac3bb2529f',
							'264645dd16bed1ee4aa37c07efd87439',
							'63284bcd98e48f9332386fb5a1393d03')
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

def Groups(t):         
    if t <0.5:
        status='Inactive'  
    else:
        status= 'Active'
    return status


def ratio(a,b):
    listo=[]
    for k in range(len(a)):
        tot = round(b[k]/a[k],2)
        listo.append(tot)
    return listo


def Target(x,y):
    listo=[]
    for i in range(len(x)):
        if x[i] == 'Inactive' and y[i] <= 0.19:
            target = 0
        elif x[i] == 'Inactive' and y[i] > 0.19:
            target = 1
        elif x[i] == 'Active' and y[i] > 0.78 :
            target = 1
        else:
            target = 0
        listo.append(target)
    return listo
          
def Normalized(a,b):
    if a == 'Inactive':
        result = 0.25*b
    else:
        result = 0.75 *(0.35 + b) -(0.0188) #variance of error is mathematically calculated
    return round(result,5)
    
def Engagement(x):
    if x <= 0.0001:
        status ='No Engagement'
    elif x >0.0001 and x <= 0.25:
        status = 'Low Engagement'
    elif x >0.25 and x<=0.5:
        status = 'Medium Engagement'
    elif x>0.5 and x<= 0.75:
        status = 'High Engagement'
    elif x > 0.75:
        status = 'Very High Engagement'
    return status
        
         
    


dfall['Isopen'] = pivot_index(dfall['CampaignIdOpen'])
countOpen = dfall[dfall['Isopen']> 0].groupby('EmailAddress')['Isopen'].sum().reset_index()
dfall['SentDate'] = dateformat(dfall['SentDate'])
dfall['DateOpen'] = dateformat(dfall['DateOpen'])
dfall['Difference'] = date_check(dfall['SentDate'].tolist(),dfall['DateOpen'].tolist())
dfall['Diff_days_to_open'] = cleandays(dfall['Difference'].tolist())
dfall=dfall[~dfall.BirthDate.str.startswith('0')]
dfall['Age']= dateformat2(dfall.BirthDate.tolist())
dfall['Sex']= Sex(dfall.Gender)

dfall_sentDate = dfall[['EmailAddress','Sex','SentDate']]
dfall_age = dfall[['EmailAddress','Age']]
dfall_day2open = dfall[['EmailAddress','Diff_days_to_open']]
sentdf = dfall_sentDate.groupby(['EmailAddress','Sex']).aggregate([min, max]).reset_index()
sentdf.columns=['EmailAddress','Sex','MinSentDate','MaxSentDate']

agedf = dfall_age.groupby('EmailAddress').mean().round().reset_index()
agedf.columns=['EmailAddress','Age']


opendf = dfall_day2open.groupby('EmailAddress').aggregate([np.mean,len]).reset_index()

opendf.columns =['EmailAddress','Avg.days_2_open','Number_campaign_sent']
sentdf['Day2camp'] = date_check(sentdf['MinSentDate'].tolist(),sentdf['MaxSentDate'].tolist())

df=pd.pivot_table(dfall,'Isopen','EmailAddress','CampaignId').reset_index()



df_temp = pd.merge(opendf,sentdf, on='EmailAddress', how='inner').merge(agedf,on='EmailAddress',how='inner')
df=df.merge(df_temp, on='EmailAddress', how='inner')
df = df.merge(countOpen, on='EmailAddress',how='outer')
df=df.fillna(0)

df= df.drop(['MinSentDate','MaxSentDate','Day2camp'],axis=1)#,'Avg.days_2_open'
df = df[(df.Age > 0) & (df.Age < 100) ]

df['Ratio'] = ratio(df['Number_campaign_sent'].tolist(),df['Isopen'].tolist())
df['Groups']= df['Ratio'].apply(Groups)
df['Target'] = Target(df['Groups'].tolist(),df['Ratio'].tolist())

def PropensityModel(df):
        listo=[]
        df2 = df.columns.get_values()
        names = df2.tolist()
        
        ind2remove = ['EmailAddress','Groups','Number_campaign_sent','Isopen','Avg.days_2_open']#,'Avg.days_2_open','Number_campaign_sent','Age','Sex','Isopen','Groups'
        
        names = [i for i in names if i not in ind2remove]
        group = df.Groups.unique()
        for g in group:
            dfg = df[df['Groups'] == g]

            propensity = LogisticRegression()
            propensity = propensity.fit(dfg[names[0:-1]], dfg.Target)
            output=propensity.predict_proba(dfg[names[0:-1]])
            pscore = output[:,1] # The predicted propensities by the model
            listo.append([dfg.EmailAddress,pscore])
        return listo
    
test =PropensityModel(df)

dft1 = pd.DataFrame({'EmailAddress':test[0][0], 'Propensity': test[0][1]})
dft2 = pd.DataFrame({'EmailAddress':test[1][0], 'Propensity': test[1][1]})
dft=pd.concat([dft1,dft2])
df= df.merge(dft, on='EmailAddress', how='inner')

df['Result'] = df.apply(lambda row: Normalized(row['Groups'],row['Propensity']),axis=1)
resultdf= df[['EmailAddress','Sex','Age','Number_campaign_sent','Isopen','Avg.days_2_open','Result']]
resultdf['Engaged?'] = resultdf.Result.apply(Engagement)
resultgroup= resultdf.groupby(['Engaged?','Sex','Age'])['Result'].mean()
resultgroup1= resultdf.groupby(['Engaged?','Sex','Age'])['Result'].count()
#plt.plot(resultdf.Isopen.tolist(),resultdf['Avg.days_2_open'].tolist(),'k-')
#resultdf.hist(figsize=(20,20))
#plt.show()
#plt.hist(resultdf.Result)
#plt.show()
#corr = resultdf.corr()
#sns.heatmap(corr, 
#            xticklabels=corr.columns.values,
#            yticklabels=corr.columns.values)
print("Execution Completed!")