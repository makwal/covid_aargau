#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime, timedelta
from general_settings import backdate, datawrapper_api_key


# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# ### Positivitätsrate

# In[15]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['daily']['test']


# In[22]:


df = pd.read_csv(url, low_memory=False)


# In[26]:


df_ch = df[df['geoRegion'] == 'CH'].copy()


# In[27]:


df_ch['anteil_pos_berechnet'] = df_ch['entries_pos']/df_ch['entries'] * 100


# In[28]:


df_ch['anteil_pos_7dmean_berechnet'] = df_ch['anteil_pos_berechnet'].fillna(0).rolling(7,center=True).mean()


# In[29]:


auswahl = ['AR','AI','SG','TG']


# In[30]:


df_kantone = df[df['geoRegion'].isin(auswahl)].copy()


# In[31]:


df_ost = df_kantone.groupby('datum').sum()


# In[32]:


df_ost['anteil_pos_berechnet'] = df_ost['entries_pos']/df_ost['entries'] * 100


# In[33]:


df_ost['anteil_pos_7dmean_berechnet'] = df_ost['anteil_pos_berechnet'].fillna(0).rolling(7,center=True).mean()


# In[34]:


df_combined = pd.concat([df_ost[['anteil_pos_7dmean_berechnet']].iloc[190:,:], df_ch.set_index('datum')[['anteil_pos_7dmean_berechnet']].iloc[190:,:]],axis=1)


# In[35]:


df_combined.columns = ['Ostschweiz','Schweiz']


# In[ ]:


#export to csv
df_combined.to_csv('/root/covid_aargau/data/ostschweiz/och_positivity.csv')


# In[73]:


latest_date = pd.to_datetime(df_combined.index.max())
latest_date = latest_date + timedelta(days=1)
latest_date = latest_date.strftime('%d.%m.%Y')


# In[ ]:


chart_id = 'B21dS'


# In[ ]:


annotation = f'Aktualisiert am {latest_date}.'


# In[ ]:


def chart_updater(chart_id, annotation):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': annotation}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
chart_updater(chart_id, annotation)


# In[ ]:





# ### Fälle

# In[41]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['daily']['cases']


# In[42]:


df = pd.read_csv(url)


# In[44]:


df_ch = df[df['geoRegion'] == 'CH'].copy()


# In[45]:


df_ch['entries-7dmean-berechnet'] = df_ch['entries'].fillna(0).rolling(7,center=True).mean()


# In[46]:


auswahl = ['AR','AI','SG','TG']


# In[47]:


df_kantone = df[df['geoRegion'].isin(auswahl)]


# In[48]:


df_ost = df_kantone.groupby('datum').sum()


# In[49]:


df_ost['entries-7dmean-berechnet'] = df_ost['entries'].fillna(0).rolling(7,center=True).mean()


# In[54]:


df_combined = pd.concat([df_ost['entries-7dmean-berechnet'], df_ch.set_index('datum')['entries-7dmean-berechnet']],axis=1)


# In[56]:


df_combined.columns = ['Ostschweiz','Schweiz']


# In[62]:


url_bev = files['csv']['rawData']['populationAgeRangeSex']


# In[63]:


auswahl = ['AR','AI','SG','TG', 'CH']


# In[64]:


df_bev = pd.read_csv(url_bev)


# In[65]:


df_bev = df_bev[df_bev['geoRegion'].isin(auswahl)].groupby('geoRegion').sum().T


# In[66]:


df_bev['OCH'] = df_bev.iloc[:,[0,1,3,4]].sum(axis=1)


# In[67]:


df_combined['Ostschweiz'] = df_combined['Ostschweiz']/df_bev.iloc[0,-1] * 100000


# In[68]:


df_combined['Schweiz'] = df_combined['Schweiz']/df_bev.iloc[0,2] * 100000


# In[ ]:


#export to csv
df_combined.to_csv('/root/covid_aargau/data/ostschweiz/och_cases.csv')


# In[73]:


latest_date = pd.to_datetime(df_combined.index.max())
latest_date = latest_date + timedelta(days=1)
latest_date = latest_date.strftime('%d.%m.%Y')


# In[ ]:


chart_id = 'FpGzL'


# In[ ]:


annotation = f'Aktualisiert am {latest_date}.'


# In[ ]:


def chart_updater(chart_id, annotation):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': annotation}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#call function
chart_updater(chart_id, annotation)

