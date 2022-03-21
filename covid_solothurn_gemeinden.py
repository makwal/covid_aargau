#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate, datawrapper_api_key
from time import sleep


# In[ ]:


#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[ ]:


file_url = 'https://corona.so.ch/fileadmin/corona/Bevoelkerung/Daten/Zahlen_nach_Gemeinden_/Zahlen_Gemeinden_{}.csv'


# In[ ]:


def request(date):
    r = requests.get(file_url.format(date))
    return r


# In[ ]:


today = datetime.now()

for i in range(7):
    current_date = (today - timedelta(i)).strftime('%d%m%Y')
    
    response = request(current_date)
    if response.status_code != 200:
        sleep(5)
        pass
    else:
        correct_url = file_url.format(current_date)
        break


# In[ ]:


temp_url = 'https://corona.so.ch/fileadmin/corona/Bevoelkerung/Daten/Zahlen_nach_Gemeinden_/Zahlen_Gemeinden_1903_21032022.csv'


# In[ ]:


#read csv-file from Excel-file
df = pd.read_csv(temp_url, delimiter=';', encoding='latin')

#save last updated time
last_updated = df['Unnamed: 2'].loc[0]

#keep all columns for now, but only relevant rows
df = df.iloc[5:, :].copy()

#keep only relevant columns and rename them
df = df[['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7']].copy()
df.columns = ['Bezirk', 'Gemeinde', 'Total Fälle', 'Fälle neu', 'Einwohner']

#Fill Bezirk column
df['Bezirk'] = df['Bezirk'].ffill()

#sort out not-needed Gemeinde data
df = df[df['Gemeinde'].notna()].copy()
df = df[df['Gemeinde'] != 'Keine Angabe'].copy()
df = df[df['Gemeinde'] != 'Übrige Gemeinden'].copy()
df = df[df['Einwohner'].notna()].copy()

#Rename Gemeinde in order to merge with wappen
df['Gemeinde'] = df['Gemeinde'].str.replace(' SO', '')
df['Gemeinde'] = df['Gemeinde'].str.replace('Nuglar-St.Pantaleon', 'Nuglar-St. Pantaleon')
df['Gemeinde'] = df['Gemeinde'].str.replace('Feldbrunnen-St.Niklaus', 'Feldbrunnen-St. Niklaus')
df['Total Fälle'] = df['Total Fälle'].str.replace('\'', '')
df['Einwohner'] = df['Einwohner'].str.replace('\'', '')


# In[ ]:


df['Total Fälle'] = df['Total Fälle'].astype(int)
df['Fälle neu'] = df['Fälle neu'].astype(int)
df['Einwohner'] = df['Einwohner'].astype(int)


# In[ ]:


#calculate incidence
df['fälle_pro_hundert'] = (df['Total Fälle'] / df['Einwohner'] * 100)


# In[ ]:


#import Wappen
#home ../Vorlagen/solothurner_gemeinden_wappen_2021.csv
#Server /root/covid_aargau/solothurner_gemeinden_wappen_2021.csv
df_wappen = pd.read_csv('/root/covid_aargau/solothurner_gemeinden_wappen_2021.csv')
df_wappen.rename(columns={'Codes:': 'bfs_nummer'}, inplace=True)
df_wappen['gemeinden'] = df_wappen['gemeinden'].str.replace(' \(SO\)', '')
df_wappen['gemeinden'] = df_wappen['gemeinden'].str.replace('Welschenrohr-Gänsbrunnen', 'Welschenrohr')
df_wappen['gemeinden'] = df_wappen['gemeinden'].str.replace('Wangen bei Olten', 'Wangen b. Olten')


# In[ ]:


#Merge and reorder columns
df = df.merge(df_wappen, left_on='Gemeinde', right_on='gemeinden', how='left')
df = df[['bfs_nummer', 'Bezirk', 'Gemeinde', 'Total Fälle', 'Fälle neu', 'Einwohner', 'fälle_pro_hundert', 'wappen']].copy()


# In[ ]:


#make a backup export of the current data
df.to_csv('/root/covid_aargau/backups/daily_data/fallzahlen_gemeinden_SO_{}.csv'.format(backdate(0)), index=False)

#export to csv
df.to_csv('/root/covid_aargau/data/only_AG/fallzahlen_gemeinden_SO.csv', index=False)


# **Chart update**

# In[ ]:


chart_id = 'flsCx'


# In[ ]:


def chart_updater(chart_id, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'describe': {'intro': f'Stand: {last_updated}'}
    }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
chart_updater(chart_id, last_updated)

