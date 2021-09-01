#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate, datawrapper_api_key
from datetime import timedelta


# ### data import

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
url = files['csv']['weeklyVacc']['byAge']['vaccPersonsV2']
df = pd.read_csv(url)


# ### data preparation

# In[ ]:


df = df[df['type'] == 'COVID19AtLeastOneDosePersons'].copy()
df = df[['date', 'geoRegion', 'altersklasse_covid19', 'per100PersonsTotal']].copy()


# In[ ]:


#format date from ISO week to regular date (monday per each week)
df['date'] = df['date'].astype(str) + '-1'
df['date'] = pd.to_datetime(df['date'], format='%Y%W-%w')

#keep only latest date
latest = df['date'].max()
df = df[df['date'] == latest].copy()
last_updated = latest + timedelta(days=6)
last_updated = last_updated.strftime('%d.%m.%Y')

#formatting
df['per100PersonsTotal'] = df['per100PersonsTotal'].round(1)
df.reset_index(inplace=True, drop=True)


# In[ ]:


def geoRegion(canton):
    dfc = df[df['geoRegion'] == canton][['altersklasse_covid19', 'per100PersonsTotal']].copy()
    #export to csv
    dfc.to_csv(f'/root/covid_aargau/data/vaccination_age/vacc_age_{canton}.csv', index=False)


# In[ ]:


cantons = ["AG", "SO", "SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR", "CH"]

for canton in cantons:
    geoRegion(canton)


# ### Datawrapper update

# In[ ]:


ids = {
    'AG': 'FoMPZ',
    'SO': 'DMhCM',
    'LU': 'hdLPP',
    'ZG': '1smMs',
    'SZ': 'xOo0R',
    'NW': 'cKJ93',
    'UR': 'CWd5i',
    'CH': 'qZ7sS'
}


# In[ ]:


def chart_updater(chart_id, date):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'Stand der Daten: {last_updated}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
for key, value in ids.items():
    chart_updater(value, last_updated)
    sleep(3)

