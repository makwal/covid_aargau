#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key
from datetime import timedelta


# ### data import

# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weeklyVacc']['byAge']['vaccPersonsV2']
df = pd.read_csv(url)


# ### data preparation

# In[4]:


df = df[(df['type'] == 'COVID19AtLeastOneDosePersons') | (df['type'] == 'COVID19FirstBoosterPersons')].copy()
df = df[['date', 'geoRegion', 'altersklasse_covid19', 'per100PersonsTotal', 'type']].copy()


# In[5]:


#format date from ISO week to regular date (monday per each week)
df['date'] = df['date'].astype(str) + '-1'
df['date'] = pd.to_datetime(df['date'], format='%Y%W-%w')

#formatting
df['per100PersonsTotal'] = df['per100PersonsTotal'].round(1)


# In[6]:


def geoRegion(canton, vacc_type):
    dfc = df[(df['geoRegion'] == canton) & (df['type'] == vacc_type)].copy()
    
    #keep only latest date
    latest = dfc['date'].max()
    dfc = dfc[dfc['date'] == latest].copy()
    last_updated = latest + timedelta(days=6)
    last_updated = last_updated.strftime('%d.%m.%Y')

    dfc.reset_index(inplace=True, drop=True)
    
    #exclude age groups
    age_group_unwanted = ['12 - 15', '16 - 64', '65+']
    dfc = dfc[~dfc['altersklasse_covid19'].isin(age_group_unwanted)].copy()
    
    dfc = dfc[['altersklasse_covid19', 'per100PersonsTotal']].copy()
    
    example_num = dfc[dfc['altersklasse_covid19'] == '80+']['per100PersonsTotal'].tail(1).values[0]
    
    which_vaccine = ''
    
    if vacc_type == 'COVID19AtLeastOneDosePersons':
        which_vaccine = 'mindestens eine Impfung'
    elif vacc_type == 'COVID19FirstBoosterPersons':
        which_vaccine = 'eine Auffrischimpfung'
    
    example = f'Lesebeispiel: Von allen über 80-Jährigen haben {example_num} Prozent {which_vaccine} erhalten'
    
    #export to csv
    #dfc.to_csv(f'/root/covid_aargau/data/vaccination_age/vacc_age_{canton}_{vacc_type}.csv', index=False)
    
    return last_updated, example


# ### Datawrapper update

# In[7]:


chart_ids = {
    'COVID19AtLeastOneDosePersons':
    {
        'AG': 'FoMPZ',
        'SO': 'DMhCM',
        'LU': 'hdLPP',
        'ZG': '1smMs',
        'SZ': 'xOo0R',
        'NW': 'cKJ93',
        'OW': 'haoMI',
        'UR': 'CWd5i',
        'CH': 'qZ7sS',
        'SG': 'n3H3t',
        'TG': 'yrGu3',
        'AR': 'Zt9WY',
        'AI': '9JTCx'
    },
    'COVID19FirstBoosterPersons':
    {
        'AG': 'tJV6i',
        'SO': 'UhZ9r',
        'LU': 'rvs1R',
        'ZG': 'peCJj',
        'SZ': 'z7VGv',
        'NW': 'bhNew',
        'OW': 'T4jti',
        'UR': 'OectS',
        'CH': 'YbzKg',
        'SG': 'khVQm',
        'TG': 'YQyiL',
        'AR': 'uGPnC',
        'AI': 'PdHhh'
    }
    
}


# In[8]:


def chart_updater(chart_id, last_updated, example):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'{example}. Stand der Daten: {last_updated}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[9]:


def main_function(canton, vacc_type, chart_id):
    last_updated, example = geoRegion(canton, vacc_type)
    chart_updater(chart_id, last_updated, example)


# In[ ]:


for vacc_type, values in chart_ids.items():
    for canton, chart_id in values.items():
        main_function(canton, vacc_type, chart_id)

