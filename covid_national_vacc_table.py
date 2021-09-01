#!/usr/bin/env python
# coding: utf-8

# # Impf-Fortschritt pro Kanton für Tabelle und einzelne stacked bar charts

# In[8]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key


# ### data import

# In[9]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[10]:


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
df = df[(df['type'] == 'COVID19PartiallyVaccPersons') | (df['type'] == 'COVID19FullyVaccPersons')][['date', 'geoRegion', 'pop', 'per100PersonsTotal', 'type']].copy()


# ### Tabelle

# In[11]:


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
df_final.to_csv("/root/covid_aargau/backups/vacc_ch/backup_vacc_table_{}.csv".format(backdate(0)), index=False)

#export to csv
df_final.to_csv("/root/covid_aargau/data/vaccination/vacc_table.csv", index=False)


# ### Einzlne Bar Charts

# In[17]:


df_final_bar_charts = df_final.copy()
df_final_bar_charts.rename(columns={'Kanton ^aktualisiert^': 'Kanton'}, inplace=True)
del df_final_bar_charts['Bevölkerung']
df_final_bar_charts['ungeimpft'] = 100 - df_final_bar_charts['zweifach Geimpfte'] - df_final_bar_charts['einfach Geimpfte']
df_final_bar_charts['Kanton'] = df_final_bar_charts['Kanton'].str.split(' ').str[0]
df_final_bar_charts.set_index('Kanton', inplace=True)


# In[18]:


cantons = {'SO': 'BPReI',
           'LU': 'IfSDu',
           'ZG': 'ssI2F',
           'SZ': 'BwUOU',
           'OW': 'EdT3V',
           'NW': 'x4UdV',
           'UR': 'JFFS3',
          'SG': 'c5q2b',
          'TG': 'SluZ7',
          'AR': '6vyiL',
          'AI': '6Mdpd',
          'AG': 'zpi9A'}


# In[29]:


for cantonskuerzel, chart_id in cantons.items():
    df_canton = df_final_bar_charts.loc[[cantonskuerzel]]
    df_canton.rename_axis(None, inplace=True)
    df_canton.index = ['Impf-Fortschritt']
    df_canton =  df_canton.round(1)
    
    zweifach_geimpft = df_canton['zweifach Geimpfte'].values[0].round(1)
    einfach_geimpft = df_canton['einfach Geimpfte'].values[0].round(1)
    ungeimpft = df_canton['ungeimpft'].values[0].round(1)
    
    
    note = f'<span style="color:#07850d">{zweifach_geimpft} Prozent der Bevölkerung sind zweifach geimpft</span>, <span style="color:#51c14b">{einfach_geimpft} Prozent einfach</span> und <span style="color:#808080">{ungeimpft} Prozent ungeimpft</span>'
    
    def chart_updater(chart_id, date):

        url_update = datawrapper_url + chart_id
        url_publish = url_update + '/publish'

        payload = {

        'metadata': {'annotate': {'notes': f'{note}. Stand: {str(date)}.'}}

        }

        res_update = requests.patch(url_update, json=payload, headers=headers)

        sleep(3)

        res_publish = requests.post(url_publish, headers=headers)

    #call function
    chart_updater(chart_id, date)
    
    #make a backup export of the current data
    df_canton.to_csv('/root/covid_aargau/backups/vacc_ch/backup_{}_{}.csv'.format(cantonskuerzel, backdate(0)))
    
    #export to csv
    df_canton.to_csv('/root/covid_aargau/data/vaccination/vacc_{}.csv'.format(cantonskuerzel))

