#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests
import pandas as pd
from time import sleep
from general_settings import backdate
import numpy as np
import dw
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# **Datenbezug**

# In[2]:


base_url = 'https://www.covid19.admin.ch/api/data/context'

res = requests.get(base_url)

res = res.json()

source_date = pd.to_datetime(res['sourceDate'])

source_date_normal = source_date.strftime('%-d. %B %Y')


# In[3]:


cases_url = res['sources']['individual']['csv']['weekly']['default']['cases']

df = pd.read_csv(cases_url)

df['date'] = df['datum'].astype(str) + '-7'


# In[4]:


def dataframe_maker(canton, df_func):

    df_func['date'] = pd.to_datetime(df_func['date'], format='%G%V-%u')

    df_func = df_func[df_func['geoRegion'] == canton].copy()
    
    df_func = df_func[['date', 'entries']].copy()
    
    df_func.set_index('date', inplace=True)
    
    return df_func


# **Skript starten**

# In[5]:


cantons = {
    'CHFL': 'U33Ka',
    'AG': 'IN8fu',
    'SO': 'K214p',
    'SG': '6waQp',
    'TG': '8K8bC',
    'AR': 'QUvs5',
    'AI': '3pN5h',
    'LU': 'DOCEA',
    'ZG': 'YRA10',
    'SZ': 'cMZdI',
    'OW': 'LkZa2',
    'NW': '04bTU',
    'UR': 'CcO8h'
}


# In[6]:


payload = {

    'metadata': {
        'annotate': {'notes': f'Aktualisiert am {source_date_normal}.'}
        }

    }


# In[7]:


for canton, chart_id in cantons.items():
    df_canton = dataframe_maker(canton, df)

    dw.data_uploader(chart_id=chart_id, df=df_canton)

    dw.chart_updater(chart_id, payload=payload)

