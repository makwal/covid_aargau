#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime, timedelta
import numpy as np
from general_settings import backdate, datawrapper_api_key


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
url = files['csv']['daily']['hospVaccPersons']
df = pd.read_csv(url)


# In[4]:


#look only at fully vaccinated people
df = df[df['vaccination_status'] == 'fully_vaccinated'].copy()

df['date'] = pd.to_datetime(df['date'])

#get only latest data
date_max = df['date'].max()
date_str = date_max.strftime('%d.%m.%Y')
df_latest = df[df['date'] == date_max].copy()

#calculate incidence
df_latest['Spitaleintritte'] = (df_latest['sumTotal'] / df_latest['pop'] * 100000).round(1)


# In[5]:


rename_dict = {
    'moderna': 'Moderna',
    'pfizer_biontech': 'Pfizer-Biontech',
    'johnson_johnson': 'Johnson & Johnson'
}


# In[6]:


df_final = df_latest[['vaccine', 'Spitaleintritte']].copy()
df_final = df_final[df_final['vaccine'] != 'all'].copy()
df_final['vaccine'] = df_final['vaccine'].map(rename_dict)
df_final.sort_values(by='Spitaleintritte', ascending=False, inplace=True)
df_final.set_index('vaccine', inplace=True)


# In[7]:


#get max value and corresponding vaccine name for Lesebeispiel
max_vaccine = df_final.index[0]
max_value = df_final.iloc[0].values[0]


# In[ ]:


#export backup to csv
df_final.to_csv('/root/covid_aargau/backups/vacc_ch/vacc_break_hosp_{}.csv'.format(backdate(0)))

#export to csv
df_final.to_csv('/root/covid_aargau/data/vaccination/vacc_break_hosp.csv')


# **Datawrapper-Update**

# In[8]:


chart_ids = {
    'spital': 'YMGUN'
}


# In[9]:


intro_hosp = f'''Lesebeispiel: Von 100\'000 mit {max_vaccine} vollständig geimpften Personen                     wurden bisher {max_value} hospitalisiert. Stand der Daten: {date_str}'''

notes_hosp = 'Die Daten basieren auf den Informationen, die Ärztinnen und Spitäler dem BAG aufgrund der Meldepflicht übermitteln.'


# In[10]:


def chart_updater(chart_id, intro_hosp, notes_hosp):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'describe': {'intro': intro_hosp},
        'annotate': {'notes': notes_hosp}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[11]:


chart_updater(chart_ids['spital'], intro_hosp, notes_hosp)

