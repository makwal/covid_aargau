#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import dw
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# **Basis-Informationen**

# In[2]:


base_url = 'https://www.covid19.admin.ch/api/data/context'


# **Datenbezug**

# In[3]:


res = requests.get(base_url)

res = res.json()

last_updated = pd.to_datetime(res['sourceDate']).strftime('%-d. %B %Y')


# In[4]:


file_url = res['sources']['individual']['csv']['vaccPersonsV2']

df = pd.read_csv(file_url)


# In[5]:


def data_handler(canton, df_func):

    df_func = df_func[(df_func['geoRegion'] == canton)
              & (df_func['type'] == 'COVID19AtLeastOneDosePersons')
              & (df_func['age_group'] == 'total_population')
             ].copy()

    df_func = df_func.set_index('date')[['per100PersonsTotal']].rename(columns={'per100PersonsTotal': 'mind. 1x geimpft'})
    
    df_func = df_func.tail(1).T

    df_func.columns = ['Impffortschritt']

    return df_func


# **Datawrapper-Update**

# In[6]:


cantons = {
    'CH': 'oRQAr',
    'AG': 'lbB3W',
    'SO': 'anaGu',
    'SG': 'JmrKq',
    'TG': '63kue',
    'AR': '0kJpq',
    'AI': 'xMysy',
    'LU': 'ooHxO',
    'ZG': '3HGzx',
    'SZ': 'qEoF9',
    'NW': 'FclSs',
    'OW': 'pol1z',
    'UR': 'Nzzu8'
}


# In[7]:


payload = {

    'metadata': {
        'annotate': {'notes': f'Aktualisiert am {last_updated}.'}
        }

    }


# In[8]:


for canton, chart_id in cantons.items():
    
    df_canton = data_handler(canton, df)
    
    dw.data_uploader(chart_id=chart_id, df=df_canton)
    
    dw.chart_updater(chart_id=chart_id, payload=payload)

