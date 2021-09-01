#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from general_settings import backdate, datawrapper_api_key
from time import sleep
from datetime import datetime


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
url = files['csv']['vaccDosesAdministered']
df_import = pd.read_csv(url)


# In[4]:


#choose only canton and necessary columns
df = df_import[df_import['geoRegion'] == 'CH'].copy()
df = df[['date', 'sumTotal']].copy()

#calculate daily administered doses
df['verabreichte Dosen'] = df['sumTotal'].diff()

df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

#use resampling to calulate weekly sum
df_final = pd.DataFrame(df.resample('W')['verabreichte Dosen'].sum())
df_final = df_final['2021-01-31':]

#Prevent current week from appearing in dataframe
today = datetime.today()
df_final = df_final[df_final.index < today].copy()


# In[ ]:


#make a backup export of the current data
df_final.to_csv('/root/covid_aargau/backups/vacc_ch/backup_CH_weekly_{}.csv'.format(backdate(0)))

#export to csv
df_final.to_csv('/root/covid_aargau/data/vaccination/vacc_CH_weekly.csv')


# **Datawrapper update**

# In[6]:


chart_id = 'qDQY3'
date = datetime.today().date().strftime('%d.%m.%Y')


# In[8]:


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

