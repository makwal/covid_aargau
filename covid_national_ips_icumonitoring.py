#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
import io
from time import sleep
from datetime import datetime, timedelta, date
import numpy as np
from general_settings import backdate, datawrapper_api_key, icumonitoring_email, icumonitoring_pw


# In[2]:


user = icumonitoring_email
password = icumonitoring_pw

data_url = 'https://polybox.ethz.ch/remote.php/webdav/Shared/outputAA/national_trends_{}.xlsx'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[9]:


def request(date):
    r = requests.get(data_url.format(date), auth=(user, password))
    return r


# In[4]:


today = date.today()

for i in range(7):
    current_date = (today - timedelta(i)).strftime('%d%m%Y')

    response = request(current_date)
    if response.status_code != 200:
        sleep(5)
        pass
    else:
        break


# In[5]:


df = pd.read_excel(io.BytesIO(response.content), sheet_name='Adults')
df.set_index(pd.to_datetime(df['date']), inplace=True)
df = df['2020-10':][['Patients', 'PatientsCOVID19', 'BedsinService', 'BedsCertified']].copy()
df.columns = ['IPS-Patienten', 'davon Covid-19-Patienten', 'Bettenkapazität', 'Zertifizierte Betten']

date = df.index[-1].strftime('%d.%m.%Y')


# In[ ]:


#backup
df.to_csv('/root/covid_aargau/backups/icumonitoring/icumonitoring_{}'.format(backdate(0)))

#export to csv
df.to_csv(f'/root/covid_aargau/data/covid_national_icumonitoring.csv')


# **Datawrapper-Update**

# In[8]:


chart_id = '3F9aW'

def chart_updater(chart_id, date):
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
        'describe': {'intro': f'Stand: {date}'}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[9]:


chart_updater(chart_id, date)

