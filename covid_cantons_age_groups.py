#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate, datawrapper_api_key
from datetime import datetime, timedelta


# In[2]:


base_url = "https://www.covid19.admin.ch/api/data/context"

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weekly']['byAge']['cases']
df_import = pd.read_csv(url)


# In[4]:


#calculate cases per 1000 people
df_import['Fälle pro 1000 Personen'] = df_import['entries'] / df_import['pop'] * 1000


# In[5]:


def data_wrangler(df_import, canton):
    df = df_import[(df_import['geoRegion'] == canton) & (df_import['altersklasse_covid19'] != 'Unbekannt')].copy()
    
    df['datum'] = df['datum'].apply(lambda x: datetime.strptime(str(x) + '-1', "%G%V-%u") + timedelta(days=6))
    
    end_date = df['datum'].tail(1).values[0]
    start_date = pd.to_datetime(end_date) - timedelta(weeks=7)
    
    df = df[(df['datum'] >= start_date) & (df['datum'] <= end_date)].copy()
    
    df = df[['datum', 'altersklasse_covid19', 'entries']].copy()
    
    df = df.pivot(index='altersklasse_covid19', columns='datum', values='entries')
    
    df.columns = ['woche1', 'woche2', 'woche3', 'woche4', 'woche5', 'woche6', 'woche7', 'woche8']
    
    df['Fälle'] = df['woche8']
    
    df = df[['Fälle', 'woche1', 'woche2', 'woche3', 'woche4', 'woche5', 'woche6', 'woche7', 'woche8']].copy()
    
    df.rename_axis('Altersgruppen', inplace=True)
    
    #get highest num of cases in all age groups for datawrapper chart purposes
    max_cases = df['Fälle'].max()
    
    df.to_csv(f'/root/covid_aargau/data/age/age_cases_{canton}.csv')
    
    return end_date, max_cases


# In[6]:


cantons = {
    'AG': '5TMAy',
    'SO': 'VHrI6',
    'LU': 'sHTIM',
    'ZG': 'E45tu',
    'SZ': 'wFbNT',
    'NW': 'CQW6q',
    'OW': 'Q6Z2T',
    'UR': 'IsKlK',
    'SG': 'VlKFG',
    'TG': 'nrLHz',
    'AR': 'mZ060',
    'AI': 'Kk8Cy',
    'CH': '1qq5e'
}


# **Datawrapper-Update**

# In[7]:


def chart_updater(chart_id, max_cases, intro):
    
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'visualize': {
            'columns': {
                'woche1': {
                    'sparkline': {
                        'rangeMax': str(max_cases)
                    }
                }
            }
        },
        'describe': {'intro': intro}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[8]:


def main_function(df_import, canton, chart_id):
    end_date, max_cases = data_wrangler(df_import, canton)
    
    end_date = pd.to_datetime(end_date)
    start_date = end_date - timedelta(days=6)
    
    end_date = end_date.strftime('%d.%m.%Y')
    start_date = start_date.strftime('%d.%m.')
    
    intro = f'Anzahl Fälle in der Woche vom {start_date} bis {end_date}.'
    
    chart_updater(chart_id, max_cases, intro)


# In[9]:


for canton, chart_id in cantons.items():
    main_function(df_import, canton, chart_id)
    sleep(2)

