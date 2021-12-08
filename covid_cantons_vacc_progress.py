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


# In[4]:


#choose only fully and partially vacc. people
df = df_import[(df_import['type'] == 'COVID19FullyVaccPersons') | (df_import['type'] == 'COVID19PartiallyVaccPersons') | (df_import['type'] == 'COVID19FirstBoosterPersons')].copy()

#formatting
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)


# In[9]:


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

    #calculate how many people are not vaccinated
    df_temp['ungeimpft'] = 100 - df_temp['COVID19FullyVaccPersons'] - df_temp['COVID19PartiallyVaccPersons']
    
    #additional columns for datawrapper
    df_temp['fully+partVacc'] = df_temp['COVID19FullyVaccPersons'] + df_temp['COVID19PartiallyVaccPersons']
    df_temp['hundred'] = 100

    df_temp = df_temp.round(1)
    
    #export to csv
    df_temp.to_csv(f'/root/covid_aargau/data/vaccination/vacc_{canton}_{age_group}.csv')

    #create note for datawrapper
    fully_vacc = df_temp['COVID19FullyVaccPersons'].values[0]
    one_vacc = df_temp['COVID19PartiallyVaccPersons'].values[0]
    no_vacc = df_temp['ungeimpft'].values[0]
    booster_vacc = df_temp['COVID19FirstBoosterPersons'].values[0]
    
    if age_group == 'total_population':
        note = f'<span style="color:#07850d">{fully_vacc} Prozent der Bevölkerung sind vollständig geimpft</span>, <span style="color:#51c14b">{one_vacc} Prozent einfach</span> und {no_vacc} Prozent ungeimpft. {booster_vacc} Prozent haben eine Booster-Impfung erhalten'
    elif age_group == '12+':
        note = f'<span style="color:#07850d">{fully_vacc} Prozent der über 12-Jährigen sind vollständig geimpft</span>, <span style="color:#51c14b">{one_vacc} Prozent einfach</span> und {no_vacc} Prozent ungeimpft. {booster_vacc} Prozent haben eine Booster-Impfung erhalten'
    else:
        note = ''
    
    return last_updated, note


# In[10]:


#cantons and corresponding chart_ids
cantons = {
    'CH': {
        'total_population': 'VjNOG',
        '12+': '7au6n'
    },
    'AG': {
        'total_population': '1jg0c',
        '12+': 'l7asf'
    },
    'SO': {
        'total_population': '8cf9Y',
        '12+': 'TGNq1'
    },
    'LU': {
        'total_population': 'uveAp',
        '12+': '6Fbgb'
    },
    'ZG': {
        'total_population': 'YB5z5',
        '12+': '4uVNw'
    },
    'SZ': {
        'total_population': 'BcHEE',
        '12+': 'Mofrn'
    },
    'OW': {
        'total_population': 'oMcgD',
        '12+': 'XfDWb'
    },
    'NW': {
        'total_population': '4drzM',
        '12+': 'MsQkB'
    },
    'UR': {
        'total_population': 'vhPsq',
        '12+': 'TZXIe'
    },
    'SG': {
        'total_population': 'DKUsi',
        '12+': 'QaS07'
    },
    'TG': {
        'total_population': 'PAaZS',
        '12+': 'h5Z8F'
    },
    'AR': {
        'total_population': 'DjA0L',
        '12+': 'bMiw4'
    },
    'AI': {
        'total_population': 'R9fid',
        '12+': '4eQDW'
    },
    'BS': {
        'total_population': 'kZcSq'
    },
    'BL': {
        'total_population': 'Y7K8E'
    }
}


# **Datawrapper-Update**

# In[11]:


def chart_updater(chart_id, note, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'{note}. Stand: {str(last_updated)}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[12]:


#call function chart_updater for every canton and age_group
for canton, canton_dict in cantons.items():
    for age_group, chart_id in canton_dict.items():
        last_updated, note = data_wrangler(canton, age_group)
        chart_updater(chart_id, note, last_updated)
        sleep(3)

