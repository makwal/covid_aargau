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

# In[92]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weekly']['default']['test']


# In[93]:


df = pd.read_csv(url, low_memory=False)


# In[94]:


df_ch = df[df['geoRegion'] == 'CH'].copy()


# In[95]:


df_ch['anteil_pos_berechnet'] = df_ch['entries_pos']/df_ch['entries'] * 100


# In[96]:


auswahl = ['AR','AI','SG','TG']


# In[97]:


df_kantone = df[df['geoRegion'].isin(auswahl)].copy()


# In[98]:


df_ost = df_kantone.groupby('datum').sum()


# In[99]:


df_ost['anteil_pos_berechnet'] = df_ost['entries_pos']/df_ost['entries'] * 100


# In[104]:


df_combined = pd.concat([df_ost[['anteil_pos_berechnet']], df_ch.set_index('datum')[['anteil_pos_berechnet']]],axis=1)


# In[107]:


df_combined = df_combined.iloc[13:,:].copy()


# In[108]:


df_combined.columns = ['Ostschweiz','Schweiz']


# In[ ]:


#export to csv
df_combined.to_csv('/root/covid_aargau/data/ostschweiz/och_positivity.csv')


# In[54]:


latest_date = datetime.today()
latest_date = latest_date.strftime('%d.%m.%Y')


# In[ ]:


chart_id = 'B21dS'


# In[ ]:


annotation = f'Aktualisiert am {latest_date} mit Zahlen der Vorwoche. Die aktuellsten Zahlen sind mit Vorsicht zu geniessen. Nachmeldungen können das Bild verändern.'


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

# In[110]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['weekly']['default']['cases']


# In[111]:


df = pd.read_csv(url)


# In[112]:


df_ch = df[df['geoRegion'] == 'CH'].iloc[:-1,:].copy()


# In[113]:


auswahl = ['AR','AI','SG','TG']


# In[114]:


df_kantone = df[df['geoRegion'].isin(auswahl)]


# In[115]:


df_ost = df_kantone.groupby('datum').sum()


# In[116]:


df_combined = pd.concat([df_ost['entries'], df_ch.set_index('datum')['entries']],axis=1)


# In[118]:


df_combined.columns = ['Ostschweiz','Schweiz']


# In[119]:


url_bev = files['csv']['rawData']['populationAgeRangeSex']


# In[120]:


auswahl = ['AR','AI','SG','TG', 'CH']


# In[121]:


df_bev = pd.read_csv(url_bev)


# In[122]:


df_bev = df_bev[df_bev['geoRegion'].isin(auswahl)].groupby('geoRegion').sum().T


# In[123]:


df_bev['OCH'] = df_bev.iloc[:,[0,1,3,4]].sum(axis=1)


# In[124]:


df_combined['Ostschweiz'] = df_combined['Ostschweiz']/df_bev.iloc[0,-1] * 100000


# In[125]:


df_combined['Schweiz'] = df_combined['Schweiz']/df_bev.iloc[0,2] * 100000


# In[ ]:


#export to csv
df_combined.to_csv('/root/covid_aargau/data/ostschweiz/och_cases.csv')


# In[73]:


latest_date = datetime.today()
latest_date = latest_date.strftime('%d.%m.%Y')


# In[ ]:


chart_id = 'FpGzL'


# In[ ]:


annotation = f'Aktualisiert am {latest_date} mit Zahlen der Vorwoche. Die aktuellsten Zahlen sind mit Vorsicht zu geniessen. Nachmeldungen können das Bild verändern.'


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

