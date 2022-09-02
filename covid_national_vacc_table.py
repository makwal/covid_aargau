#!/usr/bin/env python
# coding: utf-8

# # Impf-Fortschritt Schweiz, Kantone

# generiert folgende Grafiken:
# - Tabelle mit dem Impf-Fortschritt aller Kantone
# - Einzel-Grafiken stacked bars Impf-Fortschritt pro Kanton
# - Einzel-Grafiken stacked bars Impf-Fortschritt in der Schweiz

# In[1]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key


# ### data import

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
df = pd.read_csv(url)


#formatting, choose latest date
df['date'] = pd.to_datetime(df['date'])
latest = df['date'].max()
df = df[df['date'] == latest].copy()

df['date'] = df['date'].dt.strftime('%d.%m.%Y')
df = df[df['age_group'] == 'total_population'].copy()
df = df[(df['type'] == 'COVID19PartiallyVaccPersons') | (df['type'] == 'COVID19FullyVaccPersons')][['date', 'geoRegion', 'pop', 'per100PersonsTotal', 'type']].copy()


# ### Tabelle

# In[4]:


canton_list = df['geoRegion'].unique().tolist()
canton_list.remove('CHFL')
canton_list.remove('FL')

canton_data = []

for canton in canton_list:
    df_temp = df[df['geoRegion'] == canton].copy()
    date = df_temp['date'].values[0]
    pop = df_temp['pop'].values[0]
    
    people_two_vacc_rel = df_temp[df_temp['type'] == 'COVID19FullyVaccPersons']['per100PersonsTotal'].values[0]
    people_one_vacc_rel = df_temp[df_temp['type'] == 'COVID19PartiallyVaccPersons']['per100PersonsTotal'].values[0]
    people_no_vacc_rel = 100 - people_two_vacc_rel - people_one_vacc_rel


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
#df_final.to_csv("/root/covid_aargau/backups/vacc_ch/backup_vacc_table_{}.csv".format(backdate(0)), index=False)

#export to csv
df_final.to_csv("/root/covid_aargau/data/vaccination/vacc_table.csv", index=False)

