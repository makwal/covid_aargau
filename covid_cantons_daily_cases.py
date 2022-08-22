#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate, datawrapper_api_key
from datetime import timedelta


# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["daily"]["cases"]
df_import = pd.read_csv(url1)


# In[4]:


def daily_cases(canton):    
    #get canton and relevant columns
    df = df_import[(df_import["geoRegion"] == canton)][["datum", "entries", "mean7d"]].copy()
    
    df['datum'] = pd.to_datetime(df['datum'])
    
    #get last updated
    last_updated = df[df['entries'].notna()]['datum'].max()
    last_updated = last_updated.strftime('%d.%m.%Y')
    
    df.columns = ["Datum", "Fälle", "7-Tages-Durchschnitt"]
    
    #add a baseline (for visualization purposes in Datawrapper)
    df["baseline"] = 0
 
    #export to csv
    df.to_csv("/root/covid_aargau/data/daily_cases/daily_cases_{}.csv".format(canton), index=False)
    
    return last_updated


# In[5]:


cantons = {
    'AG': 'lw8D0',
    'SO': 'LX6pQ',
    'LU': 'S3Gy8',
    'ZG': 'pbYaD',
    'SZ': 'vRdEV',
    'NW': 'bkZIP',
    'OW': '2EnZo',
    'UR': 'hznlY',
    'SG': 'p4y22',
    'TG': 'aub8Y',
    'AR': '6tvXm',
    'AI': 'uASNx'
}


# **Datawrapper-Update**

# In[6]:


def chart_updater(chart_id, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'describe': {'intro': f'Aktualisiert am {last_updated}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[7]:


def main_function(canton, chart_id):
    last_updated = daily_cases(canton)
    chart_updater(chart_id, last_updated)


# In[8]:


for canton, chart_id in cantons.items():
    main_function(canton, chart_id)
    sleep(3)

