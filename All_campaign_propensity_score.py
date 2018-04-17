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
where  cr.CampaignId not in ('aedb06e6efcf6d99a4f339bd8e17b73a',
'4d5e0c31f2afdc8c2d3db15eb908a303',
'398e8da96d5b3244c02340170f1e1476',
'd4f1e7d3acbd7eb91aab740a978f1d05',
'd3aaf4265e4878a25fa3453b3c869803',
'8df83b64d45beb8bb52b6da3f3afc716',
'1dab620793fc354b6b98482747f71a03',
'91a1152a4224ec4f9966ee3805a37ac9',
'9b2c55c65282bab6e82d773e54055c22',
'58230221e7e6a5528399e723a751a9e9',
'17a7ceeb95c15515c9ae9ef53e99d49e',
'a9de3150769bebefed1d22044349cc84',
'08a23871dceb028bdf134b9f54f5bf98',
'bcc28899fb69e8e70eaa52833366009b',
'5ab812c10a537aa36760576f138804cf',
'ff23e9e8d9cfa2f62492c7818488fd84',
'a7c3f8ebb92d850c96ea6b6f567f7da7',
'ce9ea697b57400085f0d524757de674a',
'1c0e5990ebdcdba502da0e3a8fc1b6e7',
'f48604fbc9b5891a9f9e94ec926d22a9',
'4c6258f8ee60e03e452599c9b3222ed5',
'31323db7a06c8a84d2702c20c9082748',
'bbb8a628430ef952fcb4b7fe61b56b94',
'14620830be04fbab4f812cdd43a592e7',
'b6253d7d7efbd658b7593c75d900d119',
'2cccc92c17c81943704bb94d31c08c8f',
'b0f08f6f4416e0efae89795d0d21033a',
'79051c3cf9f90518a342865692bb30f0',
'8aa381c03be22389a3faa2f5a33aae5c',
'e4b6693ffed23063a673981b0266660c',
'83ce0586183adb0f40e2a04dd556e008',
'1f915dd7902fc535d3102013f055f329',
'fc5f0fcc07bb9f239fe820d0b002eec4',
'6509ee6947826a4c9ae86f9da9f9860c',
'746b4218c0fd9c170b0a94f86f4c01ec',
'9f0a28414ddbaeb6d3ea9a76e059261b',
'248076ec5f82838ee849c0959734ce56',
'a83a0b5c3d1928f796e850feade03aed',
'212bea1e2f97ccc6fcaabcefa025e18f',
'53c1ed7505a186c3443a9c410b50c7c4',
'd363101335891a348138877fa416e911',
'be1bf3c1bd4b84e644884deb955912f4',
'4ab9691c15b8d390804e730f3eedfab6',
'2c2e22f0a77454e5728d21bde05bdfaa',
'8c33c4682c2079b1e4cd81f6b7b2b168',
'a1a31ec727e141b61b06d991a26ee39b',
'8720a2b0a8c997ed341349f89265d36f',
'46abaa9303a6e45dbd2fd6f595bf3044',
'3e59442f23aee75cf15d2630de6c15c8')  
 and (wgc.Gender is not Null and wgc.Gender <> ' ') and (wgc.BirthDate is not Null and wgc.BirthDate <> ' ')
                              
""", conn1)

conn1.commit()
conn1.close()


dfall=dfall.drop_duplicates(['EmailAddress','CampaignId','SentDate'],keep='first')
dfall['CampaignIdOpen']=dfall['CampaignIdOpen'].fillna(0)



def Engagement(x):
    if x <= 0.01:
        status ='No Engagement'
    elif x >0.01 and x <= 0.25:
        status = 'Low Engagement'
    elif x >0.25 and x<=0.5:
        status = 'Medium Engagement'
    elif x>0.5 and x<= 0.9:
        status = 'High Engagement'
    elif x > 0.9:
        status = 'Very High Engagement'
    return status
    







campid = dfall.CampaignId.unique()
listo=[]
for c in campid:
    df = dfall[dfall.CampaignId == c]    
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
    

    
    df=df[~df.BirthDate.str.startswith('0')]
    df['Age']= dateformat2(df.BirthDate.tolist())
    df['Sex']= Sex(df.Gender)
    df['SentDate'] = dateformat(df['SentDate'])
    df['DateOpen'] = dateformat(df['DateOpen'])
    df['Difference'] = date_check(df['SentDate'].tolist(),df['DateOpen'].tolist())
    df['Difference'] = cleandays(df['Difference'].tolist())
    df['Target'] = df.CampaignIdOpen.apply(Target)
    dfn=df[['EmailAddress','Age','Sex','Difference','Target']]
    dfn = dfn[(dfn.Age > 0) & (dfn.Age < 100) ]
    
    df2 = dfn.columns.get_values()
    names = df2.tolist()
    ind2remove = ['EmailAddress']
    names = [i for i in names if i not in ind2remove]

    propensity = LogisticRegression()
    propensity = propensity.fit(dfn[names[0:-1]], dfn.Target)
    output=propensity.predict_proba(dfn[names[0:-1]])
    pscore = output[:,1] # The predicted propensities by the model
    dfn['Propensity']=pscore
    listo.append(dfn)
df_new=pd.concat(listo)

dfTotalcamp = df_new.groupby('EmailAddress').count().reset_index()
dfopen = df_new[df_new.Target != 0]
dfo = dfopen.groupby('EmailAddress').count().reset_index()
dfTotalcamp= dfTotalcamp.merge(dfo, on='EmailAddress',how='outer')  
dfTotalcamp =dfTotalcamp[['EmailAddress','Target_x','Target_y']]
dfTotalcamp['Target_y']=dfTotalcamp['Target_y'].fillna(0)
dfTotalcamp.columns=['EmailAddress','TotalCampaignSent','TotalcampaignOpen']

dfn = df_new.groupby(['EmailAddress','Age','Sex'])['Difference','Propensity'].mean().reset_index()

dfn['Engaged?'] = dfn.Propensity.apply(Engagement)

dfn = dfn.merge(dfTotalcamp,on='EmailAddress',how='inner')

 


summary0= dfn.groupby(['Sex'])['Propensity'].count()

summary1= dfn.groupby(['Engaged?'])['Propensity','Difference'].mean()
summary12= dfn.groupby(['Engaged?'])['Propensity'].count()
summary2= dfn.groupby(['Engaged?','Sex'])['Propensity','Difference'].mean()
summary21= dfn.groupby(['Engaged?','Sex'])['Propensity'].count()
summary3= dfn.groupby(['Engaged?','Age'])['Propensity','Difference'].mean()
summary31= dfn.groupby(['Engaged?','Age'])['Propensity'].count()
summary33= dfn.groupby(['Age'])['Propensity'].count()
print('Execution Completed')