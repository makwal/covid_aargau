#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from general_settings import backdate, datawrapper_api_key
from datetime import timedelta
from time import sleep


# Source for Richtwerte: https://www.bag.admin.ch/bag/de/home/krankheiten/ausbrueche-epidemien-pandemien/aktuelle-ausbrueche-epidemien/novel-cov/massnahmen-des-bundes.html#-819749076

# In[2]:


base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']

#get individual urls
url_cases = files['csv']['daily']['cases']
url_death = files['csv']['daily']['death']
url_hosp = files['csv']['daily']['hosp']
url_re = files['csv']['daily']['re']
url_ips = files['csv']['daily']['hospCapacity']


#read_csv from each url
df_cases = pd.read_csv(url_cases)
df_death = pd.read_csv(url_death)
df_hosp = pd.read_csv(url_hosp)
df_re = pd.read_csv(url_re)
df_ips = pd.read_csv(url_ips)


# In[4]:


#only keep geoRegion CH
df_cases = df_cases[df_cases['geoRegion'] == 'CH'].copy()
df_death = df_death[df_death['geoRegion'] == 'CH'].copy()
df_hosp = df_hosp[df_hosp['geoRegion'] == 'CH'].copy()
df_re = df_re[df_re['geoRegion'] == 'CH'].copy()
df_ips = df_ips[df_ips['geoRegion'] == 'CH'].copy()


# **Verschärfungen**

# In[8]:


#get relevant data from dataframes
#logic: keep all rows that are not null and get the value from the last row
inz14 = df_cases[df_cases['inzsumTotal_last14d'].notnull()]['inzsumTotal_last14d'].tail(1).values[0].round(1)
hosp7 = df_hosp[df_hosp['mean7d'].notnull()]['mean7d'].tail(1).values[0].round(1)
re = df_re[df_re['median_R_mean'].notnull()]['median_R_mean'].tail(1).values[0]
ips = df_ips[df_ips['ICU_Covid19Patients_mean15d'].notnull()]['ICU_Covid19Patients_mean15d'].tail(1).values[0].round(1)


# In[9]:


#decide if goal is met (display hook) or not (display cross)
nicht_überschritten = '<br> <em style="color:#008B8B;font-size:10px">NICHT ÜBERSCHRITTEN</em>'
überschritten = '<br> <em style="color:#FF0000	;font-size:10px">ÜBERSCHRITTEN</em>'

if inz14 < 350:
    state_inz14_v = nicht_überschritten
else:
    state_inz14_v = überschritten
    
if hosp7 < 80:
    state_hosp7_v = nicht_überschritten
else:
    state_hosp7_v = überschritten
    
if re < 1.15:
    state_re_v = nicht_überschritten
else:
    state_re_v = überschritten
    
if ips < 300:
    state_ips_v = nicht_überschritten
else:
    state_ips_v = überschritten


# In[10]:


#prepare data for dataframe
data = [
    {'Indikator': f'14-Tages-Inzidenz {state_inz14_v}', 'Aktuell': inz14, 'Richtwert': '450'},
    {'Indikator': f'R-Wert {state_re_v}', 'Aktuell': re, 'Richtwert': '1.15'},
    {'Indikator': f'IPS-Belegung (Betten, Ø 15 Tage) {state_ips_v}', 'Aktuell': ips, 'Richtwert': '300'},
    {'Indikator': f'Spitaleintritte (Ø 7 Tage) {state_hosp7_v}', 'Aktuell': hosp7, 'Richtwert': '120'}
]


# In[11]:


#final dataframe
df = pd.DataFrame(data=data)


# In[ ]:

#export to csv
df.to_csv("/root/covid_aargau/data/richtwerte/verschärfungen.csv", index=False)


# **Lockerungen**

# In[9]:


#date of last opening is used for three Richtwerte
last_opening = '2021-03-22'


# In[10]:


###inz14 is used from above
inz_l_richtwert = df_cases[df_cases['datum'] == last_opening]['inzsum14d'].tail(1).values[0].round(1)
ips_l = df_ips[df_ips['ICU_Covid19Patients'].notnull()]['ICU_Covid19Patients'].rolling(15).mean().tail(1).values[0].round(1)
re_l = df_re[df_re['median_R_mean'].notnull()]['median_R_mean'].tail(1).values[0]
hosp_l = df_hosp[df_hosp['mean7d'].notnull()]['mean7d'].tail(1).values[0].round(1)
hosp_l_richtwert = df_hosp[df_hosp['datum'] == last_opening]['mean7d'].tail(1).values[0].round(1)
death_l = df_death[df_death['mean7d'].notnull()]['mean7d'].tail(1).values[0].round(1)
death_l_richtwert = df_death[df_death['datum'] == last_opening]['mean7d'].tail(1).values[0].round(1)


# In[11]:


if inz14 < inz_l_richtwert:
    state_inz14_l = nicht_überschritten
else:
    state_inz14_l = überschritten
    
if ips_l < 250:
    state_ips_l = nicht_überschritten
else:
    state_ips_l = überschritten
    
if re_l < 1.0:
    state_re_l = nicht_überschritten
else:
    state_re_l = überschritten
    
if hosp_l < hosp_l_richtwert:
    state_hosp7_l = nicht_überschritten
else:
    state_hosp7_l = überschritten

if death_l < death_l_richtwert:
    state_death_l = nicht_überschritten
else:
    state_death_l = überschritten


# In[12]:


#prepare data for dataframe
data_l = [
    {'Indikator': f'14-Tages-Inzidenz {state_inz14_l}', 'Aktuell': inz14, 'Richtwert*': f'{inz_l_richtwert}'},
    {'Indikator': f'IPS-Belegung (Betten, Ø 15 Tage) {state_ips_l}', 'Aktuell': ips_l, 'Richtwert*': 250},
    {'Indikator': f'Aktuellster R-Wert {state_re_l}', 'Aktuell': re_l, 'Richtwert*': 1.0},
    {'Indikator': f'Spitaleintritte (Ø 7 Tage) {state_hosp7_l}', 'Aktuell': hosp_l, 'Richtwert*': f'{hosp_l_richtwert}'},
    {'Indikator': f'Todesfälle (Ø 7 Tage) {state_death_l}', 'Aktuell': death_l, 'Richtwert*': f'{death_l_richtwert}'}
]


# In[13]:


#final dataframe
df_l = pd.DataFrame(data=data_l)


# In[ ]:


#export to csv
df_l.to_csv("/root/covid_aargau/data/richtwerte/lockerungen.csv", index=False)


# **Datawrapper date update**

# Verschärfungen

# In[ ]:


#get the date of the values and today's date
inz14_date = pd.to_datetime(df_cases[df_cases['inzsum14d'].notnull()]['datum'].tail(1).values[0])
hosp7_date = pd.to_datetime(df_hosp[df_hosp['mean7d'].notnull()]['datum'].tail(1).values[0])
re_date = pd.to_datetime(df_re[df_re['median_R_mean_mean7d'].notnull()]['date'].tail(1).values[0])
ips_date = pd.to_datetime(df_ips[df_ips['ICU_Covid19Patients'].notnull()]['date'].tail(1).values[0])

today = pd.to_datetime(backdate(0))
today_str = today.strftime('%d.%m.%Y')


# In[ ]:


#add one to key if data is current
keys = 0

if inz14_date + timedelta(days=1) >= today:
    keys += 1
if hosp7_date + timedelta(days=6) >= today:
    keys +=1
if re_date + timedelta(days=15) >= today:
    keys+= 1
if ips_date + timedelta(days=1) >= today:
    keys += 1


# Lockerungen

# In[ ]:


#get the date of the values
#inz14_date from above!!!
ips_l_date = pd.to_datetime(df_ips[df_ips['ICU_Covid19Patients'].notnull()]['date'].tail(1).values[0])
re_l_date = pd.to_datetime(df_re[df_re['median_R_mean'].notnull()]['date'].tail(1).values[0])
hosp_l_date = pd.to_datetime(df_hosp[df_hosp['inzsum7d'].notnull()]['datum'].tail(1).values[0])
death_l_date = pd.to_datetime(df_death[df_death['inzsum7d'].notnull()]['datum'].tail(1).values[0])


# In[ ]:


#add one to key if data is current
keys_l = 0

if inz14_date + timedelta(days=1) >= today:
    keys_l += 1
if ips_l_date + timedelta(days=1) >= today:
    keys_l +=1
if re_l_date + timedelta(days=15) >= today:
    keys_l += 1
if hosp_l_date + timedelta(days=1) >= today:
    keys_l += 1
if death_l_date + timedelta(days=1) >= today:
    keys_l += 1


# Funktion

# In[ ]:


def chart_updater(chart_id, annotation):

    url_update = url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'Stand: {annotation}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


inz_note = '. *Als Richtwerte der 14-Tages-Inzidenz, Spitaleintritte und Todesfälle nimmt der Bundesrat die Zahlen vom 22. März (letzter Öffnungsschritt). Inzidenz bedeutet hier: auf 100\'000 Personen gerechnet.'

chart_update = {
    'verschärfungen': {'id': 'kWrh9', 'keys': keys, 'need_keys': 4, 'annotation': today_str},
    'lockerungen': {'id': 'UviDb', 'keys': keys_l, 'need_keys': 5, 'annotation': today_str + inz_note}
}


# In[ ]:


for key, value in chart_update.items():
    #if all keys are present, all chart_updater
    if value['keys'] >= value['need_keys']:
        chart_updater(value['id'], value['annotation'])

