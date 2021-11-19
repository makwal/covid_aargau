#!/usr/bin/env python
# coding: utf-8

# Source for ISO datetime handling: https://docs.python.org/3/library/datetime.html
# Look for %G, %V and %u

# Grundsätzliches Vorgehen: Die Angaben, wie viele Personen pro Alterskategorie zu welchem Zeitpunkt (Kalenderwoche) vollständig, teilweise oder nicht geimpft sind, werden mit den Hospitalisationsdaten der jeweiligen Kalenderwochen zusammengefügt. Daraus kann hernach die Inzidenz für die Alterskategorie errechnet werden (Spitaleintritte pro 100k zweifach Geimpfte resp. teilw./nicht Geimpfte.

# In[ ]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime, timedelta
import numpy as np
from general_settings import backdate, datawrapper_api_key


# In[ ]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weekly']['byAge']['hospVaccPersons']
df = pd.read_csv(url)
df = df[df['geoRegion'] == 'CHFL'].copy()


# In[ ]:


gewünschte_daten = {
                    'unter 60 Jahren': ['0 - 9', '10 - 19', '20 - 29', '30 - 39', '40 - 49', '50 - 59'],
                    'über 60 Jahren': ['60 - 69', '70 - 79', '80+']
}


# **Inzidenz-Berechnung**

# In[ ]:


def inzidenz_berechner(df, alter_key, altersklassen):
    df = df[df['altersklasse_covid19'].isin(altersklassen)].copy()

    #Fully vaccinated
    df['weight'] = df['inz_entries'] * df['pop']
    group = df.groupby(['date', 'vaccination_status'])
    df_fully = pd.DataFrame(group['weight'].sum() / group['pop'].sum()).reset_index().rename(columns={0: 'voll. geimpft'})
    df_fully = df_fully[df_fully['vaccination_status'] == 'fully_vaccinated'][['date', 'voll. geimpft']].copy()
    df_fully['voll. geimpft'] = df_fully['voll. geimpft'].round(1)

    #latest value for Datawrapper
    last_inz_double_shot = df_fully['voll. geimpft'].tail(1).values[0]
    
    #Not fully vaccinated
    df_not_fully_raw = df[(df['vaccination_status'] == 'partially_vaccinated') | (df['vaccination_status'] == 'not_vaccinated')].copy()
    df_not_fully_raw['weight'] = df_not_fully_raw['inz_entries'] * df_not_fully_raw['pop']
    group2 = df_not_fully_raw.groupby(['date'])
    df_not_fully = pd.DataFrame(group2['weight'].sum() / group2['pop'].sum()).reset_index().rename(columns={0: 'nicht voll. geimpft'})
    df_not_fully['nicht voll. geimpft'] = df_not_fully['nicht voll. geimpft'].round(1)

        
    #latest value for datawrapper
    last_inz_single_no_shot = df_not_fully['nicht voll. geimpft'].tail(1).values[0]
    
    #Merge
    df_hospvacc = pd.merge(df_fully, df_not_fully, left_on='date', right_on='date')

    #Time formatting
    df_hospvacc['date'] = df_hospvacc['date'].apply(lambda x: datetime.strptime(str(x) + '-1', "%G%V-%u") + timedelta(days=6))

    #Date for datawrapper
    date = df_hospvacc['date'].tail(1).values[0]
    date = pd.to_datetime(str(date))
    monday = date - timedelta(days=6)
    monday = monday.strftime('%d.%m.')
    sunday = date.strftime('%d.%m.%Y')

    #export backup to csv
    df_hospvacc.to_csv('/root/covid_aargau/backups/vacc_ch/hosp_vacc_{}_{}.csv'.format(alter_key, backdate(0)), index=False)

    #export to csv
    df_hospvacc.to_csv('/root/covid_aargau/data/vaccination/hosp_vacc_{}.csv'.format(alter_key), index=False)
    
    return last_inz_double_shot, last_inz_single_no_shot, monday, sunday


# **Datawrapper-Update**

# In[ ]:


def chart_updater(chart_id, intro):
    
    message = f'''Der Impfstatus ist nur bei einem Teil der Spitaleintritte bekannt.                 Die Zahl der Impfdurchbrüche wird tendenziell unterschätzt.'''
    
    intro_links = ''' <a target="_self" href="https://datawrapper.dwcdn.net/S0lQK/4/" style="background:#003595{}; padding:1px 6px; border-radius:5px; color:#ffffff; font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;"> unter 60-Jährige</a> &nbsp;

                <a target="_self" href="https://datawrapper.dwcdn.net/6MWjR/4/" style="background:#003595{}; padding:1px 6px; border-radius:5px; color:#ffffff; font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;"> über 60-Jährige</a> &nbsp;
                <br>
                <br>
                '''
    
    if chart_id == 'XnQjc':
        intro = intro_links.format('75', '', '') + intro
    elif chart_id == 'S0lQK':
        intro = intro_links.format('', '75', '') + intro
    elif chart_id == '6MWjR':
        intro = intro_links.format('', '', '75') + intro
    else:
        intro = intro_links.format('', '', '') + intro

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'annotate': {'notes': message},
        'describe': {'intro': intro}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


def lesebeispiel(monday, sunday, last_inz_double_shot, last_inz_single_no_shot, alter_key):
    
    if alter_key == 'alle':
        alter_key = ''
        
    intro = f'''Lesebeispiel: In der Woche vom {monday} bis {sunday}                 wurden {last_inz_single_no_shot} von 100\'000 Teil- oder Nichtgeimpften                 und {last_inz_double_shot} von 100\'000 Geimpften {alter_key} hospitalisiert.'''
    
    return intro


# In[ ]:


def datawrapper_main(monday, sunday, last_inz_double_shot, last_inz_single_no_shot, alter_key):
    
    chart_ids = {
    'unter 60 Jahren': 'S0lQK',
    'über 60 Jahren': '6MWjR'
    }
    
    intro = lesebeispiel(monday, sunday, last_inz_double_shot, last_inz_single_no_shot, alter_key)    
        
    chart_updater(chart_ids[alter_key], intro)


# **Haupt-Funktion**

# In[ ]:


def main_function(alter_key, altersklassen):
    
    #Inzidenz-Berechnung
    last_inz_double_shot, last_inz_single_no_shot, monday, sunday = inzidenz_berechner(df, alter_key, altersklassen)
    
    #Datawrapper-Update
    datawrapper_main(monday, sunday, last_inz_double_shot, last_inz_single_no_shot, alter_key)


# In[ ]:


for alter_key, alter_value in gewünschte_daten.items():
    main_function(alter_key, alter_value)
    sleep(5)

