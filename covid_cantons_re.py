#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from datetime import date, timedelta
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
url = files['csv']['daily']['re']
df_import = pd.read_csv(url)


# In[ ]:


def re_catcher(canton):
    #choose only AG
    df = df_import[df_import['geoRegion'] == canton].copy()

    df = df[['date', 'median_R_mean', 'median_R_highHPD', 'median_R_lowHPD']].copy()
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    #start df with first available data (28.02.2020)
    df = df['2020-02-28':].copy()
    df.columns = ['R-Median', 'Obergrenze', 'Untergrenze']

    #latest re and date for datawrapper chart
    re_latest = df[df['R-Median'].notna()]['R-Median'].tail(1).values[0]
    re_latest = str(re_latest).replace('.', ',')
    date_latest = df.index[-1]
    date = date_latest.strftime('%d.%m.%Y')

    #export to csv
    df_final.to_csv('/root/covid_aargau/data/only_AG/re_{}.csv'.format(canton))
    
    return re_latest, date


# **Datawrapper-Update**

# In[ ]:


def intro_notes_maker(re_latest, date):
    intro = f'Die aktuellste Reproduktionszahl beträgt {re_latest}.'
    notes = f'Grau = Unsicherheitsbereich. Aktualisiert am {date}.'
    
    return intro, notes

def chart_updater(chart_id, intro, notes):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'describe': {'intro': intro},
                'annotate': {'notes': notes}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


    chart_updater(chart_id, intro, notes)


# **Main-Function**

# In[ ]:


def main_function(canton, chart_id):
    re_latest, date = re_catcher(canton)
    
    intro, notes = intro_notes_maker(re_latest, date)
    
    chart_updater(chart_id, intro, notes)


# In[ ]:


chart_ids = {
    'AG': 't6Kz2',
    'SO': 'iP0C2'
}


# In[ ]:


for canton, chart_id in chart_ids.items():
    main_function(canton, chart_id)
    sleep(5)

