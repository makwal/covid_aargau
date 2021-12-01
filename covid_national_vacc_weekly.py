#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from general_settings import backdate, datawrapper_api_key
from time import sleep
from datetime import datetime, timedelta


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
url = files['csv']['vaccDosesAdministered']
df_import = pd.read_csv(url)


# In[ ]:


def data_wrangler(canton):
    #choose only canton and necessary columns
    df = df_import[df_import['geoRegion'] == canton].copy()
    df = df[['date', 'sumTotal']].copy()

    #calculate daily administered doses
    df['verabreichte Dosen'] = df['sumTotal'].diff()

    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    #use resampling to calulate weekly sum
    df_final = pd.DataFrame(df.resample('W')['verabreichte Dosen'].sum())
    df_final = df_final['2021-01-31':]

    #Prevent current week from appearing in dataframe
    today = datetime.today()
    df_final = df_final[df_final.index < today].copy()
    
    #for Datawrapper charts
    end_date = df_final.index[-1].strftime('%d.%m.%Y')
    start_date = df_final.index[-1] - timedelta(days=6)
    start_date = start_date.strftime('%d.%m.')
    latest_value = df_final['verabreichte Dosen'].tail(1).values[0].astype(int)
    
    example = f'Lesebeispiel: In der Woche vom {start_date} bis {end_date} wurden {latest_value} Impfungen verabreicht.'
    date = datetime.today().date().strftime('%d.%m.%Y')

    #export to csv
    df_final.to_csv('/root/covid_aargau/data/vaccination/vacc_{}_weekly.csv'.format(canton))

    return example, date


# **Datawrapper update**

# In[ ]:


chart_ids = {
    'CH': 'qDQY3'
}


# In[ ]:


def chart_updater(chart_id, example, date):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'describe': {'intro': example},
                 'annotate': {'notes': f'Aktualisiert: {date}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


def main_function(canton, chart_id):
    example, date = data_wrangler(canton)
    
    chart_updater(chart_id, example, date)


# In[ ]:


#call main function
for canton, chart_id in chart_ids.items():
    main_function(canton, chart_id)

