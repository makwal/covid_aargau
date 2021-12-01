#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
from datetime import datetime, timedelta
import numpy as np
from general_settings import backdate, datawrapper_api_key


# In[ ]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# **Hosp data**

# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']
url = files['csv']['daily']['hospCapacity']


# In[ ]:


df_import = pd.read_csv(url)


# In[ ]:


df_hosp = df_import[df_import['geoRegion'] == 'AG'].copy()
df_hosp = df_hosp[['date', 'ICU_AllPatients', 'ICU_Covid19Patients', 'ICU_Capacity']].copy()


# In[ ]:


df_hosp['anteil_covid_patienten'] = round((df_hosp['ICU_Covid19Patients'] / df_hosp['ICU_AllPatients'])  *100, 1)
df_hosp['belegung_icu_betten'] = round((df_hosp['ICU_AllPatients'] / df_hosp['ICU_Capacity']) * 100, 1)


# In[ ]:


anteil_covid_patienten = df_hosp['anteil_covid_patienten'].tail(1).values[0]
belegung_icu_betten = df_hosp['belegung_icu_betten'].tail(1).values[0]
verfügbare_betten = df_hosp['ICU_Capacity'].tail(1).values[0]
hosp_datum = pd.to_datetime(df_hosp['date'].tail(1).values[0]).strftime('%d.%m.%Y')


# **Re data**

# In[ ]:


re_url = files['csv']['daily']['re']
df_re = pd.read_csv(re_url)
df_re = df_re[df_re['geoRegion'] == 'AG'].copy()
df_re = df_re[['date', 'median_R_mean_mean7d']].copy()
df_re = df_re[df_re['median_R_mean_mean7d'].notnull()].copy()

re_7d = df_re['median_R_mean_mean7d'].tail(1).values[0]


# **Check if surpassed**

# In[ ]:


#decide if goal is met or not
nicht_überschritten = '<br> <em style="color:#008B8B;font-size:10px">NICHT ÜBERSCHRITTEN</em>'
überschritten = '<br> <em style="color:#FF0000;font-size:10px">ÜBERSCHRITTEN</em>'

if anteil_covid_patienten < 40:
    anteil1 = nicht_überschritten
    anteil2 = nicht_überschritten
    anteil3 = nicht_überschritten
elif anteil_covid_patienten >= 40 and anteil_covid_patienten < 50:
    anteil1 = überschritten
    anteil2 = nicht_überschritten
    anteil3 = nicht_überschritten
elif anteil_covid_patienten >= 50 and anteil_covid_patienten < 60:
    anteil1 = überschritten
    anteil2 = überschritten
    anteil3 = nicht_überschritten
elif anteil_covid_patienten >= 60:
    anteil1 = überschritten
    anteil2 = überschritten
    anteil3 = überschritten
else:
    anteil1 = ''
    anteil2 = ''
    anteil3 = ''

if belegung_icu_betten < 95:
    belegung1 = nicht_überschritten
    belegung2 = nicht_überschritten
    belegung3 = nicht_überschritten
elif belegung_icu_betten >= 95 and belegung_icu_betten < 100:
    belegung1 = überschritten
    belegung2 = nicht_überschritten
    belegung3 = nicht_überschritten
elif belegung_icu_betten >= 100:
    belegung1 = überschritten
    belegung2 = überschritten
    belegung3 = nicht_überschritten
else:
    belegung1 = ''
    belegung2 = ''
    belegung3 = ''
    
#Check, ob IPS-Zusatzbetten belegt sind. Gemäss ICU-Monitoring hat der Aargau 52 zertifizierte IPS-Betten.
#Wenn die Kapazität grösser ist, wurden Zusatzbetten geschaffen
if verfügbare_betten > 52:
    belegung1 = überschritten
    belegung2 = überschritten
    belegung3 = überschritten
    
if re_7d < 1.5:
    re = nicht_überschritten
elif re_7d >= 1.5:
    re = überschritten
else:
    re = ''


# In[ ]:


#prepare data for dataframe
data = [
    {'Indikator': 'R-Wert (Ø 7 Tage)', 'Aktuell': str(re_7d), '1. Eskalation': f'1.5 {re}', '2. Eskalation': f'1.5 {re}', '3. Eskalation (Lockdown)': f'1.5 {re}'},
    {'Indikator': 'Intensiv-Patienten mit Covid', 'Aktuell': str(anteil_covid_patienten) + '%', '1. Eskalation': f'40% {anteil1}', '2. Eskalation': f'50% {anteil2}', '3. Eskalation (Lockdown)': f'60% {anteil3}'},
    {'Indikator': 'Intensiv-Belegung', 'Aktuell': str(belegung_icu_betten) + '%', '1. Eskalation': f'95% {belegung1}', '2. Eskalation': f'100% {belegung2}', '3. Eskalation (Lockdown)': f'Intensiv-Zusatzbetten belegt  {belegung3}'},
    {'Indikator': 'Operationen', 'Aktuell': 'Spitäler verschieben nicht dringl. OP', '1. Eskalation': f'nicht dringl. OP verschoben {überschritten}', '2. Eskalation': f'dringl. und nicht dringl. OP verschoben {nicht_überschritten}', '3. Eskalation (Lockdown)': f'Notfall-OP gefährdet {nicht_überschritten}'}
]


# In[ ]:


#final dataframe
df = pd.DataFrame(data=data)


# In[ ]:


#make a backup export of the current data
df.to_csv('/root/covid_aargau/backups/daily_data/backup_eventualplanung_{}.csv'.format(backdate(0)), index=False)

#export to csv
df.to_csv('/root/covid_aargau/data/only_AG/eventualplanung.csv')


# **Datawrapper-Update**

# In[ ]:


def chart_updater(chart_id, annotation):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'Stand: {annotation}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


chart_updater('bOfbK', hosp_datum)

