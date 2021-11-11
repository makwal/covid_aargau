#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime
from general_settings import backdate, datawrapper_api_key


# In[11]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[12]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weekly']['byAge']['death']
df = pd.read_csv(url)
df = df[df['geoRegion'] == 'CHFL'].copy()


# In[13]:


#format date from ISO week to regular date (monday per each week)
df['datum_dboardformated'] = df['datum_dboardformated'] + '-1'
df['date'] = pd.to_datetime(df['datum_dboardformated'], format='%Y-%W-%w')

#define start points of waves
df.loc[df['date'] <= pd.to_datetime('2020-06-07'), 'waveNum'] = '1. Welle'
df.loc[df['date'] >= pd.to_datetime('2020-06-08'), 'waveNum'] = '2. Welle'
df.loc[df['date'] >= pd.to_datetime('2021-03-01'), 'waveNum'] = '3. Welle'
df.loc[df['date'] >= pd.to_datetime('2021-06-28'), 'waveNum'] = 'ab 4. Welle'


# Why started the 3. wave on march 1st? Until march 1st, the 7-day-average for deaths decreased for a long time (all the way down to 8.00), then it began to rise again.

# In[7]:


#keep only necessary columns
df = df[['date', 'altersklasse_covid19', 'entries', 'waveNum']].copy()

#sum up each wave and Altersklasse
df_final = pd.DataFrame(df.groupby(['altersklasse_covid19', 'waveNum'])['entries'].sum())
df_final.reset_index(inplace=True)
df_final.rename(columns={'altersklasse_covid19': 'Alter'}, inplace=True)


# In[8]:


#pivot for datawrapper reasons
df_final = df_final.pivot(index='Alter', columns='waveNum', values='entries')
df_final.drop(['Unbekannt'], inplace=True)


# In[9]:


#make a backup export of the current data
df_final.to_csv('/root/covid_aargau/backups/death/backup_death_CH_waves_{}.csv'.format(backdate(0)))

#export to csv
df_final.to_csv('/root/covid_aargau/data/death/death_CH_waves.csv')


# **Datawrapper updater**

# In[14]:


chart_id = 'WhDXn'
date = datetime.today().date().strftime('%d.%m.%Y')


# In[15]:


def chart_updater(chart_id, date):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'describe': {'intro': f'Aktualisiert: {date}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
chart_updater(chart_id, date)

