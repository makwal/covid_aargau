#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
import dw
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')

pd.set_option('display.max_columns', None)


# **Basis-Informationen**

# In[2]:


base_url = 'https://www.covid19.admin.ch/api/data/context'


# In[3]:


res = requests.get(base_url)

res = res.json()

last_updated = pd.to_datetime(res['sourceDate']).strftime('%d. %B %Y')


# In[4]:


file_url = res['sources']['individual']['csv']['daily']['hospCapacity']

df_import = pd.read_csv(file_url)

df = df_import[df_import['geoRegion'] == 'CH'].copy()

df = df[df['type_variant'] == 'fp7d'].copy()

df = df[['date', 'Total_Covid19Patients', 'ICU_Covid19Patients']].copy()

df.set_index('date', inplace=True)

df.columns = ['Hospitalisierte', 'Davon auf Intensivstation']


# **Datawrapper-Update**

# In[5]:


cantons = {
    'CH': 'ep6Kq'
}


# In[6]:


note = f'''Bei fehlenden Meldungen von Spitälern enthält der Datensatz für maximal sieben Tage den letzten gültigen Wert. Aktualisiert am {last_updated}.'''


# In[7]:


payload = {

'metadata': {
    'annotate': {'notes': note}
    }

}


# In[8]:


for canton, chart_id in cantons.items():
    
    dw.data_uploader(chart_id=chart_id, df=df)
    
    dw.chart_updater(chart_id=chart_id, payload=payload)

