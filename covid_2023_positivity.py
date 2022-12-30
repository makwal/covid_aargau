#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
import pandas as pd
import dw
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# **Basis-Informationen**

# In[9]:


base_url = 'https://www.covid19.admin.ch/api/data/context'


# **Datenbezug**

# In[10]:


res = requests.get(base_url)

res = res.json()

last_updated = pd.to_datetime(res['sourceDate']).strftime('%d. %B %Y')

file_url = res['sources']['individual']['csv']['weekly']['default']['test']

df_import = pd.read_csv(file_url)


# In[11]:


def data_handler(canton):
    df = df_import[df_import['geoRegion'] == canton].copy()

    df['date'] = pd.to_datetime(df['datum'].astype(str) + '-0', format='%G%V-%w')
    
    df = df[['date', 'anteil_pos']].copy()
    
    df.set_index('date', inplace=True)
    
    return df, last_updated


# **Datawrapper-Update**

# In[12]:


cantons = {
    'CHFL': 'qtWWg',
    'AG': '833Mg',
    'SO': '5i30e'
}


# Grafik updaten

# In[13]:


payload = {

'metadata': {
    'annotate': {'notes': f'Aktualisiert am {last_updated}.'}
    }

}


# In[14]:


for canton, chart_id in cantons.items():
    
    df_canton, last_updated = data_handler(canton)
    
    dw.data_uploader(chart_id=chart_id, df=df_canton)
    
    dw.chart_updater(chart_id=chart_id, payload=payload)

