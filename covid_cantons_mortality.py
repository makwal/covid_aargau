#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
import io
import numpy as np
from datetime import date, timedelta
from general_settings import backdate, datawrapper_api_key


# In[2]:


#url BfS
base_url = 'https://www.experimental.bfs.admin.ch/bfsstatic/dam/assets/20664214/master'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
df_import = pd.read_csv(io.StringIO(r.content.decode('latin')), delimiter=';')

#get rid of general information
df = df_import.loc[:5459].copy()

#formatting
df['Jahr'] = df['Jahr'].astype(int)
df['Woche'] = df['Woche'].astype(int)
df['endend'] = pd.to_datetime(df['endend'], format='%d.%m.%Y')

df['AnzTF_HR'] = df['AnzTF_HR'].str.strip()
df['Kanton'] = df['Kanton'].str.strip()
df['Alter'] = df['Alter'].str.strip()
df['Diff'] = df['Diff'].str.strip()

#replace '.' values with NaN 
df['AnzTF_HR'] = df['AnzTF_HR'].replace(r'^[.]$', np.nan, regex=True)
df['Diff'] = df['Diff'].replace(r'^[.]$', np.nan, regex=True)

df['AnzTF_HR'] = df['AnzTF_HR'].astype(float)
df.rename(columns={'AnzTF_HR': 'Todesfälle'}, inplace=True)


# In[4]:


def data_wrangler(df, canton, age):
    df = df[(df['Kanton'] == canton) & (df['Alter'] == age)].copy()
    df = df[['endend', 'Todesfälle', 'untGrenze', 'obeGrenze']].copy()
    
    #export to csv
    df.to_csv('/root/covid_aargau/data/death/mortality_{}_{}.csv'.format(canton, age), index=False)

    date_start = df[df['Todesfälle'].notna()]['endend'].head(1).values[0]
    date_end = df[df['Todesfälle'].notna()]['endend'].tail(1).values[0]

    date_start = pd.to_datetime(date_start).strftime('%d.%m.%Y')
    date_end = pd.to_datetime(date_end).strftime('%d.%m.%Y')
    tick_string = date_start + ', 2021-01-01, ' + date_end
    
    return date_end, tick_string


# **Datawrapper-Update**

# In[5]:


chart_ids = {
    
    'AG': {
        '0-64': 'oDe1q',
        '65+': '8Cz0z',
    },
    'SO': {
        '0-64': 'dsH5c',
        '65+': '4QNFE'
    },
    'LU': {
        '0-64': '02eSV',
        '65+': 'gSU3d'
    },
    'SG':
    {
        '0-64': 'p8JFR',
        '65+': '5CAmJ'
    }

}


# In[6]:


def chart_updater(chart_id, notes, tick_string):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'visualize': {'custom-ticks-x': tick_string},
                'annotate': {'notes': notes}
                }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[7]:


def main_function(df, canton, age, chart_id):
    date_end, tick_string = data_wrangler(df, canton, age)
    
    notes = f'<span style="color:#c71e1d">Todesfälle</span> für die letzten 40 Tage hochgerechnet. <span style="color:#989898">Graues Band:</span> Erwartbare Anzahl Todesfälle aufgrund der vorangegangenen Jahre. Stand der Daten: {date_end}'
    
    chart_updater(chart_id, notes, tick_string)


# In[8]:


for canton, chart_info in chart_ids.items():
    for age, chart_id in chart_info.items():
        main_function(df, canton, age, chart_id)
        sleep(3)

