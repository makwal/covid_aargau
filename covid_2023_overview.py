#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import requests
import pandas as pd
from time import sleep
from general_settings import backdate
import numpy as np
import dw
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# In[2]:


def execution_from(ven):
    
    exec_dict = {
        'h': str(''),
        's': '/root/covid_aargau/'
    }
    
    return exec_dict[ven]


# **<font color='red'>Hier Pfad an Umgebung anpassen!</font>**

# In[3]:


executed_from = execution_from('s')


# **Basis-Informationen**

# In[4]:


base_url = 'https://www.covid19.admin.ch/api/data/context'


# **API-Zugriff für Metadaten**

# In[5]:


res = requests.get(base_url)

res = res.json()


# Datumsangaben in allen Formaten abspeichern, die in diesem File gebraucht werden.

# In[6]:


source_date = pd.to_datetime(res['sourceDate'])

source_date_short = source_date.strftime('%d.%m.')
source_date_reg = source_date.strftime('%Y-%m-%d')
source_date_normal = source_date.strftime('%d. %B %Y')

data_version_curr = res['dataVersion']


# **Aktuelle Datenversion in separatem File speichern**

# Wir holen die aktuelle Version des Versionen-Files.

# In[7]:


df_versions = pd.read_csv(executed_from + 'version_history.csv')


# Wir holen die Angaben der Vorwoche.

# In[8]:


data_version_prev = df_versions.tail(1)['data_version'].values[0]

source_date_reg_prev = df_versions.tail(1)['date'].values[0]


# **Datenbezug**

# In[9]:


start_url = 'https://www.covid19.admin.ch/api/data/'

cases_url = '/sources/COVID19Cases_geoRegion.csv'
hosp_url = '/sources/COVID19Hosp_geoRegion.csv'
death_url = '/sources/COVID19Death_geoRegion.csv'


# Mit dieser Funktion werden die benötigten urls gebildet.

# In[10]:


def url_maker(version, data_type):
    return start_url + version + data_type


# Mit dieser Funktion werden die aktuellen Daten sowie jene der Vorwoche abgeholt und die Differenz gebildet.

# In[11]:


def data_handler(data_type):
    
    df_curr  = pd.read_csv(url_maker(data_version_curr, data_type))

    curr = df_curr[(df_curr['geoRegion'] == canton) & (df_curr['datum'] == source_date_reg)]['entries_diff_last'].values[0]

    df_prev  = pd.read_csv(url_maker(data_version_prev, data_type))

    prev = df_prev[(df_prev['geoRegion'] == canton) & (df_prev['datum'] == source_date_reg_prev)]['entries_diff_last'].values[0]

    if prev > 0:
        diff = ((curr - prev) / prev * 100).round(0)
        diff = int(diff)
    else:
        diff = np.nan

    if diff > 0:
        diff = '+' + str(diff) + '%'
    elif diff < 0:
        diff = str(diff) + '%'
    elif diff == 0:
        diff = np.nan
    else:
        pass
    
    sleep(2)
    
    return curr, prev, diff


# In dieser Funktion werden die Daten zu einem Dataframe zusammengesetzt.

# In[12]:


def dataframe_maker(canton):
    
    #Neuinfektionen    
    cases_curr, cases_prev, cases_diff = data_handler(cases_url)
    
    #Spitaleintritte
    hosp_curr, hosp_prev, hosp_diff = data_handler(hosp_url)
    
    #Todesfälle
    death_curr, death_prev, death_diff = data_handler(death_url)
    
    #Daten zu neuem df zusammensetzen
    
    if canton == 'CHFL':
        header_intro = '**Letzte Meldungen des BAG** ^über 7 Tage (inkl. Nachmeldungen)^'
    else:
        header_intro = f'**Letzte Meldungen des BAG für {canton} ** ^über 7 Tage (inkl. Nachmeldungen)^'

    header_row = {
        'Was': header_intro,
        'Wert': f'**{source_date_short}**',
        'Vgl': '**Vorwoche**',
        'Veraenderung': '**+/- in %**'
    }

    cases_row = {
        'Was': 'Neuinfektionen',
        'Wert': cases_curr,
        'Vgl': cases_prev,
        'Veraenderung': cases_diff
    }

    hosp_row = {
        'Was': 'Spitaleintritte',
        'Wert': hosp_curr,
        'Vgl': hosp_prev,
        'Veraenderung': hosp_diff
    }

    death_row = {
        'Was': 'Todesfälle',
        'Wert': death_curr,
        'Vgl': death_prev,
        'Veraenderung': death_diff
    }

    data_final = [header_row, cases_row, hosp_row, death_row]

    df_data = pd.DataFrame(data=data_final)
    
    df_data.set_index('Was', inplace=True)
    
    return df_data


# **Datawrapper-Update**

# Grafik updaten

# In[13]:


note = f'''Bei Spitaleintritten und Todesfällen ist Covid nicht in allen Fällen die Hauptursache. Aktualisiert am {source_date_normal}.'''

payload = {

    'metadata': {
        'annotate': {'notes': note}
        }

    }


# **Skript starten**

# In[14]:


cantons = {
    'CHFL': 'xBVJB',
    'AG': 'jL3o4',
    'SO': 'CHvN9',
    'SG': 'vSA2h',
    'TG': 'pjP8V',
    'AR': 'wyYq1',
    'AI': 'XN26Q',
    'LU': 'Qjy4J',
    'ZG': 'gRitN',
    'SZ': 'TLSZL',
    'OW': 'AYJX0',
    'NW': 'K6nrB',
    'UR': 'F6KYR'
}


# Falls die aktuellste Versionennummer und das Datum noch nicht in der Versions-History sind, werden die Funktionen ausgeführt.

# In[15]:


cond1 = source_date_reg not in df_versions['date'].unique()
cond2 = data_version_curr not in df_versions['data_version'].unique()

if cond1 and cond2:
    for canton, chart_id in cantons.items():
        df_canton = dataframe_maker(canton)

        dw.data_uploader(chart_id=chart_id, df=df_canton)

        dw.chart_updater(chart_id, payload=payload)
else:
    print('Die Grafiken sind auf dem aktuellsten Stand.')


# Wenn die aktuelle Datenversionsnummer **nicht** im bestehenden File vorkommt, schreiben wir sie dazu, sonst nicht.

# In[16]:


metadata_dict = {'date': source_date_reg, 'data_version': data_version_curr}

if data_version_curr not in df_versions['data_version'].unique():
    df_meta = pd.DataFrame(data=metadata_dict, index=[999])
    
    df_versions = pd.concat([df_versions, df_meta])
    df_versions.reset_index(inplace=True, drop=True)
    
    #Backup
    df_versions.to_csv(f'{executed_from}Versionbackups/version_history_{backdate(0)}.csv', index=False)
    
    #Export
    df_versions.to_csv(executed_from + 'version_history.csv', index=False)
else:
    pass

