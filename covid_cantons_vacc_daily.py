#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from general_settings import backdate, datawrapper_api_key
from time import sleep
from datetime import datetime, timedelta


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
url = files['csv']['vaccPersonsV2']
df_import = pd.read_csv(url)


# In[4]:


def vacc_daily_cantons(canton):
    #choose only canton and necessary columns
    df = df_import[(df_import['geoRegion'] == canton) & (df_import['type'] == 'COVID19AtLeastOneDosePersons')].copy()
    df = df[['date', 'entries', 'mean7d']].copy()

    #formatting
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df.rename(columns={'mean7d': 'Erstimpfungen 7-Tages-Schnitt'}, inplace=True)
    
    #only last eight weeks
    end_date = df.index[-1]
    start_date = end_date - timedelta(weeks=8)

    df_short = df[start_date:end_date].copy()

    #export to csv
    df_short.to_csv('/root/covid_aargau/data/vaccination/vacc_daily_short_{}.csv'.format(canton))
    df.to_csv('/root/covid_aargau/data/vaccination/vacc_daily_{}.csv'.format(canton))

    return start_date, end_date


# In[5]:


cantons = ['AG', 'SO', 'SG', 'AI', 'AR', 'TG', 'LU', 'ZG', 'SZ', 'OW', 'NW', 'UR', 'CH']
start_date = ''
end_date = ''

for canton in cantons:
    start_date, end_date = vacc_daily_cantons(canton)


# **Datawrapper update**

# In[6]:


cantons_dict = {
    'AG': '7FzKa',
    'AG_long': 'Sin8g',
    'LU': 'nQAgJ',
    'ZG': 'w7E33',
    'SZ': 'CMDd4',
    'NW': 'kBCF9',
    'OW': '0pDI5',
    'UR': 'YDWRO',
    'SO': '5x9Kp',
    'SO_long': 'ag74O',
    'CH': 'roqDv'
}


# Create Ticks for Datawrapper

# In[7]:


ticks = []
ticks.append(start_date.strftime('%Y-%m-%d'))

between_date = start_date

for i in range(7):
    between_date = between_date + timedelta(days=7)
    ticks.append(between_date.strftime('%Y-%m-%d'))

ticks.append(end_date.strftime('%Y-%m-%d'))
tick_string = ', '.join(ticks)


# In[8]:


note = '''<span style="color:#003595">Blaue Linie</span>: 7-Tage-Durchschnitt der Impfungen (+/- 3 Tage). <span style="color:#989898">Graue Balken</span>: Anzahl Impfungen pro Tag. An Sonntagen wird weniger geimpft. Die Zahlen können von den Angaben des Kantons abweichen'''

def chart_updater(chart_id, end_date, tick_string):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'
    
    date = end_date.strftime('%d.%m.%Y')

    payload = {

    'metadata': {'annotate': {'notes': f'{note}. Letzter Datenstand: {date}'},
                 'visualize': {'custom-ticks-x': tick_string}
                }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[9]:


for canton, chart_id in cantons_dict.items():
    chart_updater(chart_id, end_date, tick_string)
    if canton == 'AG_long' or canton == 'SO_long':
        chart_updater(chart_id, end_date, tick_string='')

