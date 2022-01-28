#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key
from datetime import timedelta


# ### data import

# In[9]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[10]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weeklyVacc']['byAge']['vaccPersonsV2']
df = pd.read_csv(url)


# ### data preparation

# In[11]:


df = df[(df['type'] == 'COVID19FullyVaccPersons') | (df['type'] == 'COVID19PartiallyVaccPersons') | (df['type'] == 'COVID19FirstBoosterPersons')].copy()
df = df[['date', 'geoRegion', 'altersklasse_covid19', 'per100PersonsTotal', 'type']].copy()


# In[12]:


#format date from ISO week to regular date (monday per each week)
df['date'] = df['date'].astype(str) + '-1'
df['date'] = pd.to_datetime(df['date'], format='%Y%W-%w')

#formatting
df['per100PersonsTotal'] = df['per100PersonsTotal'].round(1)


# In[9]:


def geoRegion(canton, vacc_type):
    df_temp = df[df['geoRegion'] == canton].copy()
    
    #keep only latest date
    last_updated = df_temp['date'].max()
    df_temp = df_temp[df_temp['date'] == last_updated].copy()
    last_updated = last_updated.strftime('%d.%m.%Y')

    df_temp.reset_index(inplace=True, drop=True)
    
    #exclude age groups
    age_group_unwanted = ['5 - 11', '12 - 15', '16 - 64', '65+']
    df_temp = df_temp[~df_temp['altersklasse_covid19'].isin(age_group_unwanted)].copy()
    
    df_temp = df_temp[['altersklasse_covid19', 'per100PersonsTotal', 'type']].copy()
    
    #create Lesebeispiel for Datawrapper
    if vacc_type == 'vollständig geimpft':
        
        example_num = df_temp[(df_temp['altersklasse_covid19'] == '80+') & (df_temp['type'] == 'COVID19FullyVaccPersons')]['per100PersonsTotal'].tail(1).values[0]
        
        example = f'Lesebeispiel: Von den über 80-Jährigen sind {example_num} Prozent vollständig geimpft'
    
    elif vacc_type == 'Booster-Impfung':
        
        example_num = df_temp[(df_temp['altersklasse_covid19'] == '80+') & (df_temp['type'] == 'COVID19FirstBoosterPersons')]['per100PersonsTotal'].tail(1).values[0]
       
        example = f'Lesebeispiel: Von den über 80-Jährigen haben {example_num} Prozent eine Auffrischimpfung erhalten'
        
    #Pivot df df so that it fits datawrapper
    df_temp = df_temp.pivot(index='altersklasse_covid19', columns='type', values='per100PersonsTotal')
    df_temp['fully+partiallyVacc'] = df_temp['COVID19FullyVaccPersons'] + df_temp['COVID19PartiallyVaccPersons']
    
    #export to csv
    df_temp.to_csv(f'/root/covid_aargau/data/vaccination_age/vacc_age_{canton}_{vacc_type}.csv')

    return last_updated, example


# ### Datawrapper update

# In[12]:


chart_ids = {
    'vollständig geimpft':
    {
        'AG': 'LqrTl',
        'SO': 'OkZZA',
        'LU': '6Zz7Y',
        'ZG': '2wxVl',
        'SZ': 'Sd4u6',
        'NW': '4u7ny',
        'OW': 'N27FT',
        'UR': 'C7YCl',
        'CH': 'wR74S',
        'SG': 'RaINy',
        'TG': 'vTcsQ',
        'AR': 'fDiOj',
        'AI': '1upzr'
    },
    'Booster-Impfung':
    {
        'AG': 'aGgoA',
        'SO': 'xEYM4',
        'LU': 'BwRer',
        'ZG': 'aFFaj',
        'SZ': 'mcWCv',
        'NW': 'C7RzP',
        'OW': 'cZAX3',
        'UR': 'dyxXz',
        'CH': 'CiWbp',
        'SG': 'SFc97',
        'TG': 'yXWjB',
        'AR': '1Qf5V',
        'AI': 'xEUrm'
    }
    
}


# In[13]:


def chart_updater(chart_id, last_updated, example):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'{example}. Stand der Daten: {last_updated}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[14]:


def main_function(canton, vacc_type, chart_id):
    last_updated, example = geoRegion(canton, vacc_type)
    chart_updater(chart_id, last_updated, example)


# In[15]:


for vacc_type, values in chart_ids.items():
    for canton, chart_id in values.items():
        main_function(canton, vacc_type, chart_id)


# In[ ]:




