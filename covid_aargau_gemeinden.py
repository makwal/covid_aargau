#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate, datawrapper_api_key
from time import sleep


# In[2]:


#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


#read xlsx-file from Aargauer Kantonswebsite, cleaning
df = pd.read_excel(file_url, sheet_name='2.1 Daten Gemeinden')


# In[4]:


df.columns = ['bezirk', 'gemeinde', 'einwohner', 'fälle_total', 'letzte Woche', 'vorletzte Woche']
df['gemeinde'] = df['gemeinde'].str.replace(' \(AG\)', '')
df.set_index('gemeinde', inplace=True)
del df['bezirk']


# ## Interaktive Karte

# **Berechnung Fälle pro Hundert Einwohner:innen**

# In[5]:


df['fälle_total'] = df['fälle_total'].replace('Keine Publikation', np.nan)
df['letzte Woche'] = df['letzte Woche'].replace('--', np.nan)
df['vorletzte Woche'] = df['vorletzte Woche'].replace('--', np.nan)
df['fälle_total'] = df['fälle_total'].astype(float)
df['fälle_pro_hundert'] = (df['fälle_total'] / df['einwohner']) * 100


# **Kategorisierung der aktuellen Fallzahlen**

# In[6]:


df['letzte_sieben_tage'] = df['letzte Woche']

#Die Kategorie 0-3 wird zu 0, um Spalte als float behandeln zu können
df['letzte_sieben_tage'] = df['letzte_sieben_tage'].replace('0-3', 0)
df['letzte_sieben_tage'] = df['letzte_sieben_tage'].astype(float)


# Funktion category_maker weist den Fallzahlen der letzten sieben Tage eine Kategorie zu:
# 
# - 0-3 Fälle (aka 0 in Spalte letzte_sieben_tage) = 0
# - 4-10 Fälle = 1
# - 11-30 Fälle = 2
# - 31-50 = 3
# - mehr als 50 = 4
# 
# Frühere Einteilung:
# 
# - 0-3 Fälle (aka 0 in Spalte letzte_sieben_tage) = 0
# - 4-6 Fälle = 1
# - 7-9 Fälle = 2
# - mehr als 10 = 3

# In[7]:


def category_maker(elem):
    if elem == 0:
        return 0
    elif elem >= 4 and elem <= 10:
        return 1
    elif elem >= 11 and elem <= 30:
        return 2
    elif elem >= 31 and elem <= 50:
        return 3
    elif elem >= 51:
        return 4
    else:
        return np.nan

df['kategorie_sieben_tage'] = df['letzte_sieben_tage'].apply(category_maker)


# **Import Wappen + Merge**

# In[9]:


#home: '../Vorlagen/bfs-nummer_gemeinde_wappen_2020.csv'
#Server: 'root/covid_aargau/bfs-nummer_gemeinde_wappen_2020.csv'
df_wappen = pd.read_csv('/root/covid_aargau/bfs-nummer_gemeinde_wappen_2020.csv')
df_wappen['Ort'] = df_wappen['Ort'].str.replace(' \(AG\)', '')
df_wappen.set_index('Ort', inplace=True)


# In[10]:


df = pd.merge(df, df_wappen, left_index=True, right_index=True, how='left')
df.reset_index(inplace=True)
df.set_index('BFS-Nummer', inplace=True)

#Rename 'Gemeinde'-column from 'index' to 'gemeinde', drop NaN rows and define bfs-index as int
df.rename(columns={'index': 'gemeinde'}, inplace=True)
df = df[df.index.notnull()].copy()
df.index = df.index.astype(int)


# In[ ]:


#make a backup export of the current data
df.to_csv('/root/covid_aargau/backups/daily_data/fallzahlen_gemeinden_{}.csv'.format(backdate(0)))

#export to csv
df.to_csv('/root/covid_aargau/data/only_AG/fallzahlen_gemeinden.csv')


# ## Tabelle

# In[86]:


df_tab = df.sort_values(by=['letzte_sieben_tage', 'gemeinde'], ascending=[False, True]).reset_index()
tenth_value = df_tab.loc[9]['letzte_sieben_tage']


# In[87]:


if tenth_value <= 0:
    tenth_value = 1
else:
    pass

df_tab = df_tab[df_tab['letzte_sieben_tage'] >= tenth_value][['gemeinde', 'letzte Woche', 'vorletzte Woche']].copy()
df_tab.columns = ['Gemeinde', 'Infektionen letzte Woche', 'Infektionen vorletzte Woche']


# In[ ]:


#make a backup export of the current data
df_tab.to_csv('/root/covid_aargau/backups/daily_data/fallzahlen_gemeinden_tabelle_{}.csv'.format(backdate(0)), index=False)

#export to csv
df_tab.to_csv('/root/covid_aargau/data/only_AG/fallzahlen_gemeinden_tabelle.csv', index=False)


# **Date**

# In[69]:


df_date = pd.read_excel(file_url, sheet_name='1. Covid-19-Daten')
df_date.rename(columns={df_date.columns[0]: 'dates'}, inplace = True)
df_date = df_date[df_date['dates'] != 'Summe'].copy()
date = df_date['dates'][df_date['Unnamed: 1'].notnull()].tail(1).values[0]

#date + 1 = heute (sobald die Zahlen aktualisiert sind)
date = date + timedelta(days=1)
date_str = date.strftime('%d.%m.%Y')


# In[70]:


#Stand der Daten herausfinden. (Woche vom dd.MM. bis dd.MM.YYYY) Dazu brauche ich die Angaben Jahr und Kalenderwoche des akt. Datums
year = date.isocalendar()[0]

#Ich will letzte Kalenderwoche, nicht die aktuelle (darum -1)
calendar_week = date.isocalendar()[1] - 1

#Montag und Sonntag berechnen und in str umwandeln
monday = datetime.strptime(f'{year}-{calendar_week}-1', '%G-%V-%u')
sunday = monday + timedelta(days=6)

monday_str = monday.strftime('%d.%m.')
sunday_str = sunday.strftime('%d.%m.%Y')


# **Datawrapper-Update Karte**

# In[107]:


intro_map = f'Neuinfektionen Woche vom {monday_str} bis {sunday_str}'
notes_map = f'Aus Datenschutzgründen weist der Kanton 0 bis 3 Neuinfektionen als eine Kategorie aus. Für Gemeinden mit weniger als 500 Einwohnern macht er keine Angaben.'

notes_tab = 'Letzte Woche = {} bis {}'.format(monday_str, sunday_str)


# In[108]:


data_dict = {
    'karte': {
        'chart_id': 'r1Mao',
        'intro': intro_map,
        'notes': notes_map 
    },
    'tabelle': {
        'chart_id': '6OoHy',
        'intro': '',
        'notes': notes_tab
    }
}


# In[112]:


def chart_updater(chart_id, intro, notes):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'describe': {'intro': intro},
        'annotate': {'notes': notes}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
for chart_type, data in data_dict.items():
    chart_id = data['chart_id']
    intro = data['intro']
    notes = data['notes']
    chart_updater(chart_id, intro, notes)

