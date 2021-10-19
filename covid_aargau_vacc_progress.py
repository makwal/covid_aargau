#!/usr/bin/env python
# coding: utf-8

# # # Impf-Fortschritt Aargau

# source for number of adults: https://www.bfs.admin.ch/bfs/de/home/statistiken/bevoelkerung/stand-entwicklung/bevoelkerung.assetdetail.18344310.html

# In[20]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key


# In[21]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[22]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['vaccPersonsV2']
df_import = pd.read_csv(url)
#choose only AG
df = df_import[df_import['geoRegion'] == 'AG'].copy()


# In[23]:


#formatting
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)
df.rename(columns={'per100PersonsTotal': 'total_pop'}, inplace=True)

#take last row for every type
df = df[df['age_group'] == 'total_population'].groupby('type').tail(1)
last_updated = df.index[0].strftime('%d.%m.%Y')


# In[5]:


#create adult column
pop_adult = 566768

df['adults'] = ((df['sumTotal'] / pop_adult) * 100).round(2)

#create teenager colum
pop_from_teen = 607656

df['from_teen'] = ((df['sumTotal'] / pop_from_teen) * 100).round(2)


# In[16]:


df_vacc = df[(df['type'] == 'COVID19FullyVaccPersons') | (df['type'] == 'COVID19PartiallyVaccPersons')][['type', 'total_pop', 'adults', 'from_teen']].copy()

populations = ['total_pop', 'adults', 'from_teen']

for p in populations:
    df_temp = df_vacc.pivot_table(index=df_vacc.index, columns='type', values=p)
    df_temp.columns = ['vollständig geimpft', 'einfach geimpft']
    df_temp['ungeimpft'] = 100 - df_temp['vollständig geimpft'] - df_temp['einfach geimpft']
    df_temp = df_temp.round(1)
    df_temp['Impf-Fortschritt'] = 'Impf-Fortschritt'
    df_temp.reset_index(inplace=True)
    df_temp.set_index('Impf-Fortschritt', inplace=True)
    del df_temp['date']
    
    #make a backup export of the current data
    df_temp.to_csv('/root/covid_aargau/backups/vacc_ch/backup_AG_{}_{}.csv'.format(p, backdate(0)))

    #export to csv
    df_temp.to_csv('/root/covid_aargau/data/vaccination/vacc_AG_{}.csv'.format(p))


# In[18]:


#extract percentages for datawrapper notes
people_two_vacc_rel = (df_vacc[df_vacc['type'] == 'COVID19FullyVaccPersons']['total_pop'].values[0]).round(1)
people_one_vacc_rel = (df_vacc[df_vacc['type'] == 'COVID19PartiallyVaccPersons']['total_pop'].values[0]).round(1)
people_no_vacc_rel = (100 - people_two_vacc_rel - people_one_vacc_rel).round(1)

people_two_vacc_rel_adult = (df_vacc[df_vacc['type'] == 'COVID19FullyVaccPersons']['adults'].values[0]).round(1)
people_one_vacc_rel_adult = (df_vacc[df_vacc['type'] == 'COVID19PartiallyVaccPersons']['adults'].values[0]).round(1)
people_no_vacc_rel_adult = (100 - people_two_vacc_rel_adult - people_one_vacc_rel_adult).round(1)

people_two_vacc_rel_teen = (df_vacc[df_vacc['type'] == 'COVID19FullyVaccPersons']['from_teen'].values[0]).round(1)
people_one_vacc_rel_teen = (df_vacc[df_vacc['type'] == 'COVID19PartiallyVaccPersons']['from_teen'].values[0]).round(1)
people_no_vacc_rel_teen = (100 - people_two_vacc_rel_teen - people_one_vacc_rel_teen).round(1)


# In[19]:


#function chart_updater updates the date and the percentage in the annotation section and re-publishes the chart

note_regular = f'<span style="color:#07850d">{people_two_vacc_rel} Prozent der Bevölkerung sind vollständig geimpft</span>, <span style="color:#51c14b">{people_one_vacc_rel} Prozent einfach</span> und <span style="color:#808080">{people_no_vacc_rel} Prozent ungeimpft</span>'
note_adult = f'<span style="color:#07850d">{people_two_vacc_rel_adult} Prozent der Erwachsenen sind vollständig geimpft</span>, <span style="color:#51c14b">{people_one_vacc_rel_adult} Prozent einfach</span> und <span style="color:#808080">{people_no_vacc_rel_adult} Prozent ungeimpft</span>'
note_from_teen = f'<span style="color:#07850d">{people_two_vacc_rel_teen} Prozent der Bevölkerung ab 12 Jahren sind vollständig geimpft</span>, <span style="color:#51c14b">{people_one_vacc_rel_teen} Prozent einfach</span> und <span style="color:#808080">{people_no_vacc_rel_teen} Prozent ungeimpft</span>'


chart_update = {
        'vacc_all': ['zpi9A', note_regular],
        'vacc_adults': ['RJSCm', note_adult],
        'vacc_from_teen': ['5NOAH', note_from_teen]
    }


def chart_updater(chart_information, date):

    url_update = datawrapper_url + chart_information[0]
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'{chart_information[1]}. Stand: {str(date)}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function chart_updater for every chart
for key, value in chart_update.items():
    chart_updater(value, last_updated)

