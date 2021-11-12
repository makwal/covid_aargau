#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
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
url = files['csv']['vaccPersonsV2']
df_import = pd.read_csv(url)

#choose only fully and partially vacc. people
df = df_import[(df_import['type'] == 'COVID19FullyVaccPersons') | (df_import['type'] == 'COVID19PartiallyVaccPersons')].copy()

#formatting
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)


# In[5]:


#Function data_wrangler takes canton and age_group and exports the respective df

def data_wrangler(canton, age_group):
    
    #limit df to canton and age group, take last row of each type (fully/partially vacc.)
    df_temp = df[df['geoRegion'] == canton].copy()
    df_temp = df_temp[df_temp['age_group'] == age_group].groupby('type').tail(1)
    
    #last update (for datawrapper chart)
    last_updated = df_temp.index[0].strftime('%d.%m.%Y')
    
    #keep only necessary columns
    df_temp = df_temp[['per100PersonsTotal', 'type']].copy()

    #Pivot df so that it fits datawrapper
    df_temp = df_temp.pivot(columns='type', values='per100PersonsTotal')

    df_temp['Impf-Fortschritt'] = 'Impf-Fortschritt'
    df_temp.reset_index(inplace=True, drop=True)
    df_temp.set_index('Impf-Fortschritt', inplace=True)

    df_temp.columns = ['vollständig geimpft', 'einfach geimpft']

    #calculate how many people are not vaccinated
    df_temp['ungeimpft'] = 100 - df_temp['vollständig geimpft'] - df_temp['einfach geimpft']

    df_temp = df_temp.round(1)
    
    #export to csv
    df_temp.to_csv(f'/root/covid_aargau/data/vaccination/vacc_{canton}_{age_group}.csv')
    
    #create note for datawrapper
    fully_vacc = df_temp['vollständig geimpft'].values[0]
    one_vacc = df_temp['einfach geimpft'].values[0]
    no_vacc =df_temp['ungeimpft'].values[0]
    
    if age_group == 'total_population':
        note = f'<span style="color:#07850d">{fully_vacc} Prozent der Bevölkerung sind vollständig geimpft</span>, <span style="color:#51c14b">{one_vacc} Prozent einfach</span> und <span style="color:#808080">{no_vacc} Prozent ungeimpft</span>'
    elif age_group == '12+':
        note = f'<span style="color:#07850d">{fully_vacc} Prozent der Bevölkerung ab 12 Jahren sind vollständig geimpft</span>, <span style="color:#51c14b">{one_vacc} Prozent einfach</span> und <span style="color:#808080">{no_vacc} Prozent ungeimpft</span>'
    else:
        note = ''
    
    return last_updated, note


# In[4]:


#cantons and corresponding chart_ids
cantons = {
    'CH': {
        'total_population': '8hVAj',
        '12+': 'ZszeT'
    },
    'AG': {
        'total_population': 'zpi9A',
        '12+': '5NOAH'
    },
    'SO': {
        'total_population': 'BPReI',
        '12+': 'djn1E'
    },
    'LU': {
        'total_population': 'IfSDu',
        '12+': 'VeCLU'
    },
    'ZG': {
        'total_population': 'ssI2F',
        '12+': 'UajZ3'
    },
    'SZ': {
        'total_population': 'BwUOU',
        '12+': 'XVpHL'
    },
    'OW': {
        'total_population': 'EdT3V',
        '12+': '8CI4X'
    },
    'NW': {
        'total_population': 'x4UdV',
        '12+': 'tQIsb'
    },
    'UR': {
        'total_population': 'JFFS3',
        '12+': '5W5gv'
    }
}


# **Datawrapper-Update**

# In[6]:


def chart_updater(chart_id, note, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'{note}. Stand: {str(last_updated)}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[8]:


#call function chart_updater for every canton and age_group
for canton, canton_dict in cantons.items():
    for age_group, chart_id in canton_dict.items():
        last_updated, note = data_wrangler(canton, age_group)
        chart_updater(chart_id, note, last_updated)
        sleep(3)

