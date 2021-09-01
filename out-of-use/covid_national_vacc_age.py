#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate
from datetime import timedelta


# ### data import

# In[ ]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': 'Bearer exBDzRsC86QAktkFECOOvK0ZjVTDN2u1LOWq6VjdTsaHUh9mjaKJodeYRIh75F68'}


# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weeklyVacc']['byAge']['vaccPersons']
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

#remove FL and date
df = df[(df['geoRegion'] != 'CHFL') & (df['geoRegion'] != 'FL')].copy()
del df['date']

# In[ ]:


altersklassen = df['altersklasse_covid19'].unique()

for a in altersklassen:
    df_temp = df[df['altersklasse_covid19'] == a].copy()
    #make a backup export of the current data
    df_temp.to_csv('/root/covid_aargau/backups/vaccination_age/backup_age_{}_{}.csv'.format(backdate(0), a), index=False)
    #export to csv
    df_temp.to_csv('/root/covid_aargau/data/vaccination_age/vacc_age_{}.csv'.format(a), index=False)


# ### Datawrapper update

# In[ ]:


ids = {
    '0 - 9': 'A2eKW',
    '10 - 19': 'MGnev',
    '20 - 29': '6Ipug',
    '30 - 39': 'er9Ug',
    '40 - 49': 'F65uy',
    '50 - 59': 'HOdN9',
    '60 - 69': 'Kb3Pm',
    '70 - 79': '2PUsb',
    '80+': '63j2D'
}


# In[ ]:


def chart_updater(chart_id, date):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'describe': {'intro': f'Stand der Daten: {date}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
for key, value in ids.items():
    chart_updater(value, last_updated)
    sleep(3)

