# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import scipy
from scipy.stats import binom, hypergeom

import math
from sklearn.linear_model import LogisticRegression
dfa = pd.read_excel("C://Users/ogun9000/Documents/EmailCampaign/Score_propensityOpen.xlsx", sheetname='Allrecipients')
dfo = pd.read_excel("C://Users/ogun9000/Documents/EmailCampaign/Score_propensityOpen.xlsx", sheetname='Openrecipients')
dfa['EmailAddress']=dfa['EmailAddress'].str.lower()
dfo['EmailAddress']=dfo['EmailAddress'].str.lower()


df=pd.merge(dfa, dfo, on='EmailAddress', how='left')
df=df.drop_duplicates(['EmailAddress'],keep='last')
df['CampaignId_y']=df['CampaignId_y'].fillna(0)

def pivot_index(x):
    listo=[]
    
    y=x.tolist()

    for i in range(len(y)):
        if y[i] == 0:
            status = 0
        else:
            status =1
        listo.append(status)
    return listo

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
        if i<0:
            status=-1
        else:
            status =i
        listo.append(status)
    return listo
    
df['Date']=df['Date'].fillna(0)
df['Isopen'] = pivot_index(df['CampaignId_y'])
df['Difference'] = date_check(df['SentDate'].tolist(),df['Date'].tolist())
df['Diff_days'] = cleandays(df['Difference'].tolist())

df= df[['EmailAddress','Diff_days','Isopen']]
names=['Diff_days','Isopen']

#print(df[names[1:-1]])

propensity = LogisticRegression()
propensity = propensity.fit(df.Diff_days, df.Isopen)
pscore = propensity.predict_proba(df.Diff_days.tolist()) # The predicted propensities by the model
print (pscore[:5])

df['Propensity'] = pscore

#corr = df.corr()
#sns.heatmap(corr, 
#            xticklabels=corr.columns.values,
#            yticklabels=corr.columns.values)

#test= df[df['Isopen'] ==0].count()
#df.to_excel("C://Users/ogun9000/Documents/EmailCampaign/Propensity_result.xlsx")
print('Execution Completed!')