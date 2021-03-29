#!/usr/bin/env python
# coding: utf-8

# # Impf-Fortschritt pro Kanton (für Tabelle)

# In[1]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate


# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': 'Bearer exBDzRsC86QAktkFECOOvK0ZjVTDN2u1LOWq6VjdTsaHUh9mjaKJodeYRIh75F68'}


# In[3]:


#files the function gets called on
files = {'all vacc given': 'vaccDosesAdministered', 'people two vacc': 'fullyVaccPersons'}

def bag_vaccinator(vacc_status, filename):
    r = requests.get(base_url)
    response = r.json()
    files = response['sources']['individual']
    url = files['csv'][filename]
    df = pd.read_csv(url)

    #formatting, choose latest date
    df = df[df['sumTotal'].notnull()].copy()
    df['date'] = pd.to_datetime(df['date'])
    
    latest = df['date'].max()
    df = df[df['date'] == latest].copy()
    df['date'] = df['date'].dt.strftime('%d.%m.%Y')
    df['vacc_status'] = vacc_status

    return df[['date', 'geoRegion', 'pop', 'sumTotal', 'vacc_status']]


# In[4]:


#call function for each file, concat Dataframes to df
df = pd.DataFrame([])

for key, value in files.items():
    df_temp = bag_vaccinator(key, value)
    df = pd.concat([df, df_temp])
    sleep(3)
    
df.reset_index(inplace=True, drop=True)


# **calculation per canton**

# In[6]:


canton_list = df['geoRegion'].unique().tolist()
canton_list.remove('CHFL')
canton_list.remove('FL')

canton_data = []

for canton in canton_list:
    df_temp = df[df['geoRegion'] == canton].copy()
    
    #proceed only if the two dates match
    if df_temp['date'].nunique() == 1:
    
        date = df_temp['date'].iloc[0]
        pop = df_temp['pop'].iloc[0]

        people_two_vacc = df_temp['sumTotal'][df_temp['vacc_status'] == 'people two vacc'].values[0]
        people_two_vacc_rel = (people_two_vacc / pop) * 100

        people_one_vacc = df_temp['sumTotal'][df_temp['vacc_status'] == 'all vacc given'].values[0] - (people_two_vacc * 2)
        people_one_vacc_rel = (people_one_vacc / pop) * 100
        
        temp_dict = {
                    'Kanton ^aktualisiert^': f'{canton} ^{date}^',
                    'zweifach Geimpfte': people_two_vacc_rel,
                    'einfach Geimpfte': people_one_vacc_rel,
                    'Bevölkerung': pop
                    }
        canton_data.append(temp_dict)

df_final = pd.DataFrame(data=canton_data)
df_final.sort_values(by='zweifach Geimpfte', ascending=False, inplace=True)


# In[ ]:


#make a backup export of the current data
df_final.to_csv("/root/covid_aargau/backups/vacc_ch/backup_vacc_table_{}.csv".format(backdate(0)), index=False)

#export to csv
df_final.to_csv("/root/covid_aargau/data/vaccination/vacc_table.csv", index=False)
