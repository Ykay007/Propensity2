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
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors


conn1 = pyodbc.connect(
    r'DRIVER={ODBC Driver 13 for SQL Server};'
    r'SERVER=GB-UXS-WBIDV-D1;'
    r'DATABASE=PBRDMStaging;'
    r'Trusted_Connection=yes;'
    )
df_not= pandas.io.sql.read_sql("""
                          
select  x.EmailAddress,datediff(dd,x.SentDate,x.DateOpen)'Day2Open', x.Gender, x.BirthDate,x.SentDate,x.DateOpen,x.[Subject] \

from   (
		select cr.EmailAddress ,cast(convert(varchar(10),cc.SentDate,110) as date )'SentDate',cr.CampaignId,co.EmailAddress'EmailAddressOpen',cast(convert(varchar(10),co.[Date],110) as date)'DateOpen',co.CampaignId'CampaignIdOpen',cc.[Subject],wgc.Gender,wgc.BirthDate \
		,ROW_NUMBER() OVER(PARTITION BY cr.EmailAddress, cc.[Subject]  order by cc.SentDate ) corr \
		from [dbo].[CampaignMonitor_ClientCampaign] cc \
		inner join [dbo].[CampaignMonitor_CampaignRecipient] cr  on cr.CampaignID = cc.CampaignId \
		inner join [dbo].[CampaignMonitor_CampaignOpen] co on cr.CampaignId = co.CampaignId  and cr.EmailAddress = co.EmailAddress \
		inner join [PBRDM].[dbo].[W_GoldenConsumer] wgc on  wgc.Email = co.EmailAddress \
		where 
										(cc.[Subject] not like '%firstname%') and (wgc.Gender is not Null and wgc.Gender <> ' ') and (wgc.BirthDate is not Null and wgc.BirthDate <> ' '))x \
		WHERE x.corr = 1 and x.SentDate <= x.DateOpen

                               
                              
""", conn1)

conn1.commit()
conn1.close()

dfa = pd.read_excel("C://Users/ogun9000/Documents/EmailCampaign/newdata.xlsx", sheetname='openpersonified')


def Age(today,x):
        
   agin = today.year - x.year - ((today.month, today.day) < (x.month, x.day))
   return agin
    

def dateformat(x):
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

def Birth_Verification(x,e,d,s,st):
    listo=[]
    try:
        
        for i in range(len(x)):
            if x[i] > 0 and x[i]<100:
                listo.append([e[i],d[i],s[i],x[i],st[i]])
    except ValueError:
        
        print('Input Error')
    return listo

    
    
dfa['Sex']= Sex(dfa.Gender)
df_not['Sex']= Sex(df_not.Gender)
dfa['Status'] =1
df_not['Status']=0
df_not=df_not[~df_not.BirthDate.str.startswith('0')]
dfa['Age']= dateformat(dfa.BirthDate.tolist())
df_not['Age']= dateformat(df_not.BirthDate.tolist())

dfa= dfa.groupby(['EmailAddress','Sex','Status'])['Day2Open','Age'].mean().round().reset_index()
df_not= df_not.groupby(['EmailAddress','Sex','Status'])['Day2Open','Age'].mean().round().reset_index()



names=['EmailAddress','Day2Open','Sex','Age','Status']
dfa=dfa[names]
df_not=df_not[names]
dftreated = pd.DataFrame(Birth_Verification(dfa.Age.tolist(),dfa.EmailAddress.tolist(),dfa.Day2Open.tolist(),dfa.Sex.tolist(),dfa.Status.tolist() ),columns=names)
dfcontrol = pd.DataFrame(Birth_Verification(df_not.Age.tolist(),df_not.EmailAddress.tolist(),df_not.Day2Open.tolist(),df_not.Sex.tolist(),df_not.Status.tolist() ),columns=names)

data= pd.concat([dftreated,dfcontrol])


propensity = LogisticRegression()
propensity = propensity.fit(data[names[1:-1]], data.Status)
output=propensity.predict_proba(data[names[1:-1]])
pscore = output[:,1] # The predicted propensities by the model
data['Propensity'] = pscore



def Match(groups, propensity, caliper = 0.05):
   
    # Check inputs
#    if any(propensity <=0) or any(propensity >=1):
#        raise ValueError('Propensity scores must be between 0 and 1')
#    elif not(0<caliper<1):
#        raise ValueError('Caliper must be between 0 and 1')
#    elif len(groups)!= len(propensity):
#        raise ValueError('groups and propensity scores must be same dimension')
#    elif len(groups.unique()) != 2:
#        raise ValueError('wrong number of groups')
        
        
    # Code groups as 0 and 1
    groups = groups == groups.unique()[0]
    N = len(groups)
    N1 = groups.sum(); N2 = N-N1
    g1, g2 = propensity[groups == 1], (propensity[groups == 0])
    # Check if treatment groups got flipped - treatment (coded 1) should be the smaller
    if N1 > N2:
       N1, N2, g1, g2 = N2, N1, g2, g1 
        
        
    # Randomly permute the smaller group to get order for matching
    morder = np.random.permutation(N1)
    matches = pd.Series(np.empty(N1))
    matches[:] = np.NAN
    
    for m in morder:
        dist = abs(g1[m] - g2)
        if dist.min() <= caliper:
            matches[m] = dist.argmin()
            g2 = g2.drop(matches[m])
    return (matches)

stuff = Match(data.Status, data.Propensity)
g1, g2 = data.Propensity[data.Status==1], data.Propensity[data.Status==0]
#e1, e2 =data.EmailAddress[data.Status==1],data.EmailAddress[data.Status==0]

res = zip(g1, g2[stuff])
Result = list(res)
#print(Result)

###### Method 2 for propensity score Matching of Customers


def get_matching_pairs(treated_df, non_treated_df, scaler=True):

    treated_x = treated_df.values
    non_treated_x = non_treated_df.values
    if scaler == True:
        scaler = StandardScaler()
    if scaler:
        scaler.fit(treated_x)
        treated_x = scaler.transform(treated_x)
        non_treated_x = scaler.transform(non_treated_x)

    nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(non_treated_x)
    distances, indices = nbrs.kneighbors(treated_x)
    indices = indices.reshape(indices.shape[0])
    matched = non_treated_df.ix[indices]
    return matched


#matched_df = get_matching_pairs(dftreated[names[1:-1]], dfcontrol[names[1:-1]])

#fig, ax = plt.subplots(figsize=(6,6))
#plt.scatter(dfcontrol['x'], dfcontrol['y'], alpha=0.3, label='All non-treated')
#plt.scatter(dftreated['x'], dftreated['y'], label='Treated')
#plt.scatter(matched_df['x'], matched_df['y'], marker='x', label='matched')
#plt.legend()
#plt.xlim(-1,2)

print("Execution Completed!")