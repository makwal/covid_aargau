#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime, timedelta
from general_settings import backdate, datawrapper_api_key
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['wasteWater']['viralLoad']
df = pd.read_csv(url)
df['date'] = pd.to_datetime(df['date'])


# Peaks herausfiltern

# In[4]:


df_max = df.groupby('geoRegion')['vl_percentile_mean7d'].idxmax().to_frame()
df_max['max'] = 'vl_percentile_mean7d_max'


# In[5]:


df_max.reset_index(drop=True, inplace=True)
df_max.set_index('vl_percentile_mean7d', inplace=True)


# In[6]:


df = df.merge(df_max, left_index=True, right_index=True, how='left')


# In[7]:


df_max_final = df[df['max'] == 'vl_percentile_mean7d_max'].copy()
df_max_final = df_max_final[['name', 'date']].rename(columns={'date': 'max_date'})


# df auf Stationen reduzieren, die innerhalb der letzten zwei Wochen Daten geliefert haben

# In[8]:


dff = df[df['vl_percentile_mean7d'].notna()].copy()

fortnight = datetime.now() - timedelta(days=15)
dff = dff[dff['date'] > fortnight].copy()


# Formattieren

# In[9]:


dff = dff[['name', 'date', 'vl_percentile_mean7d', 'vl_percentile_mean7d_diff_pp']].copy()


# Für jede Station die letzte Zeile erhalten

# In[10]:


dff = dff.groupby('name').tail(1)


# Alle Daten zusammenmergeen

# In[11]:


df_final = dff.merge(df_max_final, left_on='name', right_on='name', how='right')


# Prozentpunktveränderung in separater Spalte so anpassen, dass alle Werte > 0 sind (für Tooltip)

# In[12]:


df_final['vl_percentile_mean7d_diff_pp_show'] = df_final['vl_percentile_mean7d_diff_pp']
df_final.loc[df_final['vl_percentile_mean7d_diff_pp_show'] < 0, 'vl_percentile_mean7d_diff_pp_show'] = df_final['vl_percentile_mean7d_diff_pp_show'] * -1


# Export

# In[ ]:


#make a backup export of the current data
df_final.to_csv('/root/covid_aargau/backups/wastewater/wastewater_{}.csv'.format(backdate(0)))

#export to csv
df_final.to_csv('/root/covid_aargau/data/wastewater/wastewater.csv')


# Datawrapper-Grafik

# In[20]:


latest_date = df['date'].max()
latest_date = latest_date + timedelta(days=1)
latest_date = latest_date.strftime('%-d. %B %Y')


# In[31]:


chart_id = 'abYKl'


# In[32]:


annotation = f'Ausgegraute Stationen: Für diese Anlagen liegen für die letzten zwei Wochen keine Daten vor. Aktualisiert am {latest_date}.'


# In[34]:


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

