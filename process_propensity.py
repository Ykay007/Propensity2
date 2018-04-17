import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

df = pd.read_excel("C://Users/ogun9000/Documents/EmailCampaign/deeper_propensity.xlsx")

dfopen= df[['CampaignId','Recipients','TotalOpened','UniqueOpened','SentDate_d','SentHour','Mentions','Forwards','Likes','Subject','Name','Rate.Days',	'Avg.Hour','MinOpenDate','MaxOpendate','Diff_Date']]

def kpi(p,q):
    x=p.tolist()
    y=q.tolist()
    listo=[]
    for i in range(len(x)):
        metric= round((x[i]/y[i])*100,1)
        listo.append(metric)
    return listo

dfopen['TopenRate'] = kpi(dfopen['TotalOpened'],dfopen['Recipients'])
dfopen['UopenRate'] = kpi(dfopen['UniqueOpened'],dfopen['Recipients'])

interval = np.arange(min(round(dfopen['UopenRate'],0)),max(round(dfopen['UopenRate'],0)),10).astype(np.int64)

def _Grouping_(data,lst):
    listp=[]
    freq=0
    freq2=0
    freq3=0
    freq4 =0
    freq5=0
    freq6=0
    freq7=0
    freq8 =0
    freq9=0
    freq10 =0    
    listo =lst
    for i in range(len(data)):
        if data[i] <= listo[0]:
            freq +=1
        elif data[i]> listo[0] and data[i] <= listo[1]:
            freq2 +=1
        elif data[i]> listo[1] and data[i] <= listo[2]:
            freq3 +=1
        elif data[i]> listo[2] and data[i] <= listo[3]:
            freq4 +=1
        elif data[i]> listo[3] and data[i] <= listo[4]:
            freq5 +=1
        elif data[i]> listo[4] and data[i] <= listo[5]:
            freq6 +=1
        elif data[i]> listo[5] and data[i] <= listo[6]:
            freq7 +=1
        elif data[i]> listo[6] and data[i] <= listo[7]:
            freq8 +=1
        elif data[i]> listo[7] and data[i] <= listo[8]:
            freq9 +=1
        elif data[i]> listo[9]:
            freq10 +=1
    listp.append([freq,freq2,freq3,freq4,freq5,freq6,freq7,freq8,freq9,freq10])
    return listp

new=_Grouping_(dfopen['UopenRate'].tolist(),interval)
weighted_avg = round(np.average(new[0][:], weights=interval),1)

def Category(x,y):
    listo=[]
    for i in range(len(x)):
        if x[i] >= y:
            category ='High'
        else:
            category='Low'
        listo.append(category)
    return listo
dfopen['Category_uniqueOpen'] = Category(dfopen['UopenRate'].tolist(),weighted_avg)

#
import nltk
#nltk.download()
from collections import Counter
from nltk.book import *

def pool_words(x):
    wordlist=[]
    listword=[]
    y = x.tolist()
    for i in y:
        wordlist.append(i.split())
    for j in wordlist:
        for k in j:
            listword.append(k.strip('[]!??&-'))
    return listword
word_subject= pool_words(dfopen['Subject'])

def NLProcess(x):
    freq=[]
    fdist = FreqDist(x)
    #print(fdist.most_common(100))
    fdist.plot(60, cumulative=False)
    freq.append(fdist.most_common(100))
    popularity_data = freq[0][:]
    return popularity_data
most_used_word = NLProcess(word_subject)

def pool_2_words(x):
    wordlist=[]
  
    y = x.tolist()
    for i in y:
        newword = i.strip('[]!??&-,')
        wordstring = ' '.join(newword.split()[:2])
        wordlist.append(wordstring)
  
    return wordlist
word_2_subject= pool_2_words(dfopen['Subject'])
most_used_2_words = NLProcess(word_2_subject)

def pool_3_words(x):
    wordlist=[]
    y = x.tolist()
    for i in y:
        newword = i.strip('[]!??&-,')
        wordstring = ' '.join(newword.split()[:3])
        wordlist.append(wordstring)
  
    return wordlist
word_3_subject= pool_3_words(dfopen['Subject'])
most_used_3_words = NLProcess(word_3_subject)

toplist = most_used_3_words[1:18]


