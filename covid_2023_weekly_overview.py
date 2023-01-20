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
source_date_normal = source_date.strftime('%-d. %B %Y')

data_version_curr = res['dataVersion']


# **Aktuelle Datenversion in separatem File speichern**

# In[7]:


df_versions = pd.read_csv(executed_from + 'version_history.csv')


# In[8]:


metadata_dict = {'date': source_date_reg, 'data_version': data_version_curr}

if data_version_curr not in df_versions['data_version'].unique():
    df_meta = pd.DataFrame(data=metadata_dict, index=[999])
    
    df_versions = pd.concat([df_versions, df_meta])
    df_versions.reset_index(inplace=True, drop=True)
    
    #Backup
    df_versions.to_csv(f'{executed_from}Versionbackups/version_history_{backdate(0)}.csv', index=False)
    
    #Export
    df_versions.to_csv(executed_from + 'version_history.csv', index=False)


# **Datenbezug**

# In[9]:


types = ['cases', 'hosp', 'death']
type_url = res['sources']['individual']['csv']['weekly']['default']


# In[10]:


def dataframe_maker(canton):
    
    #Für die Datumsangabe greife ich zuerst nur auf die Neuinfektionen zurück.
    df_date = pd.read_csv(type_url['cases'])
    
    df_date = df_date[df_date['geoRegion'] == canton].copy()
    df_date.reset_index(drop=True, inplace=True)

    iso_week_curr = int(df_date['datum_dboardformated'].values[-1].split('-')[1])
    iso_week_prev = int(df_date['datum_dboardformated'].values[-2].split('-')[1])
    
    if canton == 'CHFL':
        header_intro = '**Letzte Meldungen des BAG ^für die letzten zwei Kalenderwochen^**'
    else:
        header_intro = f'**Letzte Meldungen des BAG für {canton} ^für die letzten zwei Kalenderwochen^**'

    header_row = {
                'Was': header_intro,
                'Wert': f'**Woche {str(iso_week_curr)}**',
                'Vgl': f'**Woche {str(iso_week_prev)}**',
                'Veraenderung': '**+/- in %**'
            }
    
    data_final = [header_row]
    
    #Hier hole ich die Angaben Neuinfektionen, Hospitalisierungen und Todesfälle nacheinander.
    for t in types:
        df_temp = pd.read_csv(type_url[t])

        df_temp = df_temp[df_temp['geoRegion'] == canton].copy()

        curr = df_temp.tail(1)['entries'].values[0]
        curr = int(curr)
        
        diff = df_temp.tail(1)['entries_diff_abs'].values[0]
        diff = int(diff)
        
        prev = curr - diff
        prev = int(prev)
        
        if prev > 0:
            diff_pct = diff / prev * 100
            diff_pct = int(diff_pct)
        else:
            diff_pct = np.nan

        if diff_pct > 0:
            diff_pct = '+' + str(diff_pct) + '%'
        elif diff_pct < 0:
            diff_pct = str(diff_pct) + '%'
        elif diff_pct == 0:
            diff_pct = np.nan
        else:
            pass

        if t == 'cases':
            type_final = 'Neuinfektionen'
        elif t == 'hosp':
            type_final = 'Spitaleintritte'
        elif t == 'death':
            type_final = 'Todesfälle'

        template_row = {
            'Was': type_final,
            'Wert': curr,
            'Vgl': prev,
            'Veraenderung': diff_pct
        }
        
        data_final.append(template_row)
        
    df_final = pd.DataFrame(data=data_final)
    
    df_final.set_index('Was', inplace=True)
    
    return df_final


# **Datawrapper-Update**

# Grafik updaten

# In[11]:


note = f'''Bei Spitaleintritten und Todesfällen ist Covid nicht in allen Fällen die Hauptursache. Aktualisiert am {source_date_normal}.'''

payload = {

    'metadata': {
        'annotate': {'notes': note}
        }

    }


# **Skript starten**

# In[12]:


cantons = {
    'CHFL': 'l2HY5',
    'AG': 'cEgJN',
    'SO': 'sr8JD',
    'SG': 'moAdS',
    'TG': '2lIqx',
    'AR': 'GIBMf',
    'AI': 'r6waQ',
    'LU': 'Mn1xm',
    'ZG': 'f5wtX',
    'SZ': 'sjJss',
    'OW': '0HXIZ',
    'NW': 'dVh9c',
    'UR': '1Fr7P'
}


# In[13]:


for canton, chart_id in cantons.items():
    df_canton = dataframe_maker(canton)
    
    if canton == 'CHFL':
        header_intro = '**Letzte Meldungen des BAG**'
    else:
        header_intro = f'**Letzte Meldungen des BAG für {canton}**'


    dw.data_uploader(chart_id=chart_id, df=df_canton)

    dw.chart_updater(chart_id, payload=payload)

