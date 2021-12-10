#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from datetime import date, timedelta
from general_settings import backdate, datawrapper_api_key


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
url = files['csv']['daily']['virusVariantsWgs']
df_import = pd.read_csv(url)


# In[4]:


df = df_import[(df_import['geoRegion'] == 'CHFL') & (df_import['data_source'] == 'wgs')].copy()
df = df[(df['variant_type'] == 'B.1.1.529') | (df['variant_type'] == 'B.1.617.2')].copy()
df.set_index('date', inplace=True)


# In[5]:


df_mean = df[['variant_type', 'prct_mean7d']].copy()
df_mean = df_mean.pivot(columns='variant_type', values='prct_mean7d')


# In[6]:


df_upper = pd.DataFrame(df.groupby('variant_type')['prct_upper_ci'].rolling(7).mean()).reset_index()
df_lower = dff = pd.DataFrame(df.groupby('variant_type')['prct_lower_ci'].rolling(7).mean()).reset_index()
df_upper_lower = df_upper.merge(df_lower, left_on=['date', 'variant_type'], right_on=['date', 'variant_type'])
df_upper_lower.set_index('date', inplace=True)
df_upper_lower = df_upper_lower.pivot(columns='variant_type', values=['prct_upper_ci', 'prct_lower_ci'])
df_upper_lower.columns = ['_'.join(col) for col in df_upper_lower.columns]


# In[7]:


df_final = df_mean.merge(df_upper_lower, left_index=True, right_index=True, how='left')
df_final = df_final['2021-06':].copy()
df_final.loc[:'2021-11', 'prct_upper_ci_B.1.1.529'] = 0


# In[ ]:


#Export to csv
df_final.to_csv('/root/covid_aargau/data/variants.csv')


# In[ ]:


omicron_prct = df_final[df_final['B.1.1.529'].notna()]['B.1.1.529'].tail(1).values[0]
delta_prct = df_final[df_final['B.1.617.2'].notna()]['B.1.617.2'].tail(1).values[0]

start_date = df_final[df_final['B.1.1.529'].notna()].index[0]
end_date = df_final[df_final['B.1.1.529'].notna()].index[-1]


# **Datawrapper-Update**

# In[ ]:


chart_id = '3vqL4'


# In[ ]:


intro = f'''
Der geschätzte Anteil von <span style="background-color:#15607a;color:white">Omikron</span> 
liegt bei {omicron_prct} Prozent, jener von <span style="background-color:#fa8c00;color:white">Delta</span> 
bei {delta_prct} Prozent (gleitende 7-Tages-Durchschnitte).'''

tick_string = start_date + ', ' + end_date


# In[ ]:


def chart_updater(chart_id, tick_string, intro):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'visualize': {'custom-ticks-x': tick_string},
                    'describe': {'intro': intro}
                }

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


chart_updater(chart_id, tick_string, intro)

