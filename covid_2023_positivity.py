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
import dw
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
    
    df.set_index('date', inplace=True)
    
    return df, last_updated


# **Datawrapper-Update**

# In[5]:


cantons = {
    'CHFL': 'qtWWg',
    'AG': '833Mg',
    'SO': '5i30e'
}


# Grafik updaten

# In[6]:


payload = {

'metadata': {
    'annotate': {'notes': f'Aktualisiert am {last_updated}.'}
    }

}


# In[7]:


for canton, chart_id in cantons.items():
    
    df_canton, last_updated = data_handler(canton)
    
    dw.data_uploader(chart_id=chart_id, df=df_canton)
    
    dw.chart_updater(chart_id=chart_id, payload=payload)

