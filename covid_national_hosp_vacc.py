#!/usr/bin/env python
# coding: utf-8

# Source for ISO datetime handling: https://docs.python.org/3/library/datetime.html
# Look for %G, %V and %u

# Grundsätzliches Vorgehen: Die Angaben, wie viele Personen pro Alterskategorie zu welchem Zeitpunkt (Kalenderwoche) vollständig, teilweise oder nicht geimpft sind, werden mit den Hospitalisationsdaten der jeweiligen Kalenderwochen zusammengefügt. Daraus kann hernach die Inzidenz für die Alterskategorie errechnet werden (Spitaleintritte pro 100k zweifach Geimpfte resp. teilw./nicht Geimpfte.

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


# **Liste der Altersklassen erstellen**

# In[4]:


url_alter_vacc = files['csv']['weeklyVacc']['byAge']['vaccPersonsV2']
df_alter_vacc = pd.read_csv(url_alter_vacc)
altersklassen_liste = df_alter_vacc['altersklasse_covid19'].unique()
altersklassen_liste = altersklassen_liste[:-3]

# In[5]:


gewünschte_daten = {
                    'alle': altersklassen_liste,
                    'unter 60 Jahren': altersklassen_liste[:6],
                    'über 60 Jahren': altersklassen_liste[6:]
}


# **Impfdaten: Import der Zahlen der absolut Geimpften pro Kalenderwoche**

# In[6]:


def impf_main(altersklassen):
    url_vacc = files['csv']['weeklyVacc']['byAge']['vaccPersonsV2']
    dfvacc = pd.read_csv(url_vacc)

    #Auswahl der Altersklassen und wichtigsten Spalten
    dfvacc = dfvacc[dfvacc['altersklasse_covid19'].isin(altersklassen)].copy()
    dfvacc = dfvacc[dfvacc['geoRegion'] == 'CHFL'].copy()
    dfvacc = dfvacc[['date', 'altersklasse_covid19', 'pop', 'sumTotal', 'type']].copy()
    dfvacc.rename(columns={'sumTotal': 'totalVaccinated'}, inplace=True)
    
    #Pro Impfstatus (vollst./teilweise/mind. 1x geimpft) eine Spalte. Werte = Anzahl geimpfter Personen pro Status
    dfvacc = dfvacc.pivot(index=['date', 'altersklasse_covid19', 'pop'], columns='type', values='totalVaccinated')
    dfvacc.reset_index(inplace=True)
    
    #Berechnung, wie viele Menschen doppelt geimpft sind resp. 1x oder ungeimpft.
    dfvacc['double_shot'] = dfvacc['COVID19FullyVaccPersons']
    dfvacc['single_no_shot'] = dfvacc['COVID19PartiallyVaccPersons'] + (dfvacc['pop'] - dfvacc['COVID19AtLeastOneDosePersons'])
    dfvacc = dfvacc[['date', 'altersklasse_covid19', 'double_shot', 'single_no_shot']].copy()
    
    return dfvacc


# **Hospitalisierungs-Daten**

# Nun erfolgt Import und Bereitstellung der Hospitalisationsdaten. Es braucht zwei Importe: Einmal die Gesamtzahl Hospitalisierter (hosp) und einmal die geimpften Hospitalisierten (hospVaccPersons). Beide Datasets verfügen über einen Kalenderwochen-Intervall.

# In[7]:


def hosp_main(altersklassen):
    
    #Files, die einzulesen sind
    file_list = ['hosp', 'hospVaccPersons']

    #Liste, in der die Hospitalisationsdaten abgelegt werden, um danach gemergt zu werden.
    dataframes = []
    
    #In dieser Funktion werden beide Files in Form gebracht und danach in der dataframes-Liste abgelegt.
    def hospitalizer(file):
        url = files['csv']['weekly']['byAge'][file]
        df_hosp = pd.read_csv(url)
        df_hosp = df_hosp[df_hosp['altersklasse_covid19'].isin(altersklassen)].copy()
        df_hosp = df_hosp[df_hosp['geoRegion'] == 'CHFL'].copy()
        try:
            df_hosp = df_hosp[['altersklasse_covid19','datum', 'entries']].copy()
        except:
            df_hosp = df_hosp[['altersklasse_covid19','date', 'entries']].copy()

        df_hosp.columns = ['altersklasse_covid19', 'date', file]
        dataframes.append(df_hosp)
        sleep(5)
        
    for file in file_list:
        hospitalizer(file)
        
    #Merge dataframes
    df_hosp = pd.merge(dataframes[0], dataframes[1], left_on=['date', 'altersklasse_covid19'], right_on=['date', 'altersklasse_covid19'])

    #Neue Spalte mit den Hospitalisierten, die nicht doppelt geimpft sind
    df_hosp['hosp_single_no_vacc'] = df_hosp['hosp'] - df_hosp['hospVaccPersons']
    
    return df_hosp


# **Inzindenz-Berechnung**

# In[8]:


def inzidenz_berechner(df):
    #Datum-Formatierung: Aus Kalenderwoche mach Datum des jeweiligen Sonntags
    df['date'] = df['date'].apply(lambda x: datetime.strptime(str(x) + '-1', "%G%V-%u") + timedelta(days=6))
    
    #Datum für Datawrapper
    date = df['date'].tail(1).values[0]
    
    #Zusammenrechnen aller Altersgruppen (alle Spalten, sprich geimpfte Personen und Hospitalisierungen)
    df = df.groupby('date').sum()
    
    #Inzidenzberechnung: Hospitalisierte geteilt durch (Nicht/teilw.-)Geimpfte * Hunderttausend
    df['inz_hosp_single_no_vacc'] = (df['hosp_single_no_vacc'] / df['single_no_shot'] * 100000).round(1)
    df['inz_hospVaccPersons'] = (df['hospVaccPersons'] / df['double_shot'] * 100000).round(1)
    
    #Die relevanten Spalten behalten
    df = df[['inz_hosp_single_no_vacc', 'inz_hospVaccPersons']].copy()
    df.columns = ['nicht/teilw. geimpft', 'vollständig geimpft']
    
    #Inzidenz für Datawrapper
    last_inz_single_no_shot = df['nicht/teilw. geimpft'].tail(1).values[0]
    last_inz_double_shot = df['vollständig geimpft'].tail(1).values[0]
    
    return df, date, last_inz_single_no_shot, last_inz_double_shot


# **Export**

# In[9]:


def export(df_final, alter_key):
    #export backup to csv
    df_final.to_csv('/root/covid_aargau/backups/vacc_ch/hosp_vacc_{}_{}.csv'.format(alter_key, backdate(0)))
    
    #export to csv
    df_final.to_csv('/root/covid_aargau/data/vaccination/hosp_vacc_{}.csv'.format(alter_key))


# In[10]:


def date_handler(date):
    date = pd.to_datetime(str(date))
    monday = date - timedelta(days=6)
    monday = monday.strftime('%d.%m')
    sunday = date.strftime('%d.%m.%Y')
    
    return monday, sunday


# **Datawrapper-Update**

# In[17]:


def chart_updater(chart_id, intro):
    
    message = f'''Der Impfstatus ist nur bei einem Teil der Spitaleintritte bekannt.                 Die Zahl der Impfdurchbrüche wird tendenziell unterschätzt.'''
    
    intro_links = '''<a target="_self" href="https://datawrapper.dwcdn.net/XnQjc/4/" style="background:#003595{}; padding:1px 6px; border-radius:5px; color:#ffffff; font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;"> ganze Bevölkerung</a> &nbsp;

                <a target="_self" href="https://datawrapper.dwcdn.net/S0lQK/4/" style="background:#003595{}; padding:1px 6px; border-radius:5px; color:#ffffff; font-weight:400; box-shadow:0px 0px 7px 2px rgba(0,0,0,0.07); cursor:pointer;"> unter 60-Jährige</a> &nbsp;

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


# In[18]:


def lesebeispiel(monday, sunday, last_inz_single_no_shot, last_inz_double_shot, alter_key):
    
    if alter_key == 'alle':
        alter_key = ''
        
    intro = f'''Lesebeispiel: In der Woche vom {monday} bis {sunday}                 wurden {last_inz_single_no_shot} von 100\'000 Teil- oder Nichtgeimpften                 und {last_inz_double_shot} von 100\'000 Geimpften {alter_key} hospitalisiert.'''
    
    return intro


# In[19]:


def datawrapper_main(monday, sunday, last_inz_single_no_shot, last_inz_double_shot, alter_key):
    
    chart_ids = {
    'alle': 'XnQjc',
    'unter 60 Jahren': 'S0lQK',
    'über 60 Jahren': '6MWjR'
    }
    
    intro = lesebeispiel(monday, sunday, last_inz_single_no_shot, last_inz_double_shot, alter_key)
    
    
    chart_updater(chart_ids[alter_key], intro)


# **Haupt-Funktion**

# In[20]:


def main_function(alter_key, altersklassen):
    
    #Impfdaten
    dfvacc = impf_main(altersklassen)
    
    #Hospitalisierungsdaten
    df_hosp = hosp_main(altersklassen)
    
    #Merge von Impf- und Hospitalisierungsdaten
    df = pd.merge(dfvacc, df_hosp, left_on=['date', 'altersklasse_covid19'], right_on=['date', 'altersklasse_covid19'], how='right')
    
    #Inzidenz-Berechnung
    df_final, date, last_inz_single_no_shot, last_inz_double_shot = inzidenz_berechner(df)
    
    #Export
    export(df_final, alter_key)
    
    #Datum für Datawrapper formatieren
    monday, sunday = date_handler(date)
    
    #Datawrapper-Update
    datawrapper_main(monday, sunday, last_inz_single_no_shot, last_inz_double_shot, alter_key)


# In[21]:


for alter_key, alter_value in gewünschte_daten.items():
    main_function(alter_key, alter_value)
    sleep(5)

