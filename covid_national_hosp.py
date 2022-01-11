#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime
from general_settings import backdate, datawrapper_api_key


# In[30]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[31]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['daily']['hospCapacity']
df = pd.read_csv(url)
df = df[df['geoRegion'] == 'CH'].copy()


# In[32]:


df = df[['date', 'Total_Covid19Patients', 'ICU_Covid19Patients']].reset_index(drop=True).copy()
df.columns = ['Datum', 'Hospitalisierte', 'davon auf Intensivstation']


# In[25]:


#make a backup export of the current data
df.to_csv('/root/covid_aargau/backups/hosp/covid_national_hosp.csv'.format(backdate(0)), index=False)

#export to csv
df.to_csv('/root/covid_aargau/data/covid_national_hosp.csv', index=False)

