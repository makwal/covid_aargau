#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests
import pandas as pd
from time import sleep
from general_settings import backdate, datawrapper_api_key
from datetime import date, datetime, timedelta
import numpy as np
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# **Basis-Informationen**

# In[2]:


base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# **Datenbezug**

# In[3]:


res = requests.get(base_url)

res = res.json()

last_updated = pd.to_datetime(res['sourceDate']).strftime('%d. %B %Y')

file_url = res['sources']['individual']['csv']['weekly']['default']['test']

df_import = pd.read_csv(file_url)


# In[4]:


def data_handler(canton):
    df = df_import[df_import['geoRegion'] == canton].copy()

    df['date'] = pd.to_datetime(df['datum'].astype(str) + '-0', format='%G%V-%w')
    
    df = df[['date', 'anteil_pos']].copy()
    
    return df, last_updated


# **Datawrapper-Update**

# Daten in die Grafik laden.

# In[5]:


def data_uploader(chart_id, df_func):
    dw_upload_url = datawrapper_url + chart_id +'/data'

    datawrapper_headers = {
        'Accept': '*/*',
        'Content-Type': 'text/csv',
        'Authorization': headers['Authorization']
    }
    
    #data is being transformed to a csv
    data = df_func.to_csv(encoding='utf-8', index=False)

    response = requests.put(dw_upload_url, data=data.encode('utf-8'), headers=datawrapper_headers)

    status_code = response.status_code
    if status_code > 204:
        print(chart_id + ': ' + str(status_code))
    
    sleep(3)
    
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    res_publish = requests.post(url_publish, headers=datawrapper_headers)
    
    status_code2 = res_publish.status_code
    
    if status_code2 > 204:
        print(chart_id + ': ' + str(status_code2))


# Grafik updaten

# In[6]:


def chart_updater(chart_id, last_updated):
    
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'annotate': {'notes': f'Aktualisiert am: {last_updated}.'}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[7]:


cantons = {
    'CHFL': 'qtWWg',
    'AG': '833Mg',
    'SO': '5i30e'
}


# In[8]:


for canton, chart_id in cantons.items():
    
    df_canton, last_updated = data_handler(canton)
    
    data_uploader(chart_id, df_canton)
    
    chart_updater(chart_id, last_updated)

