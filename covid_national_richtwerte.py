#!/usr/bin/env python
# coding: utf-8

# In[39]:


import requests
import pandas as pd
from general_settings import backdate
from datetime import timedelta
from time import sleep

# In[19]:


base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': 'Bearer exBDzRsC86QAktkFECOOvK0ZjVTDN2u1LOWq6VjdTsaHUh9mjaKJodeYRIh75F68'}
chart_id = '6QlGI'


# In[20]:


r = requests.get(base_url)
response = r.json()
files = response['sources']['individual']

#get individual urls
url_cases = files['csv']['daily']['cases']
url_hosp = files['csv']['daily']['hosp']
url_re = files['csv']['daily']['re']
url_ips = files['csv']['daily']['hospCapacity']

#read_csv from each url
df_cases = pd.read_csv(url_cases)
df_hosp = pd.read_csv(url_hosp)
df_re = pd.read_csv(url_re)
df_ips = pd.read_csv(url_ips)


# In[21]:


#only keep geoRegion CH
df_cases = df_cases[df_cases['geoRegion'] == 'CH'].copy()
df_hosp = df_hosp[df_hosp['geoRegion'] == 'CH'].copy()
df_re = df_re[df_re['geoRegion'] == 'CH'].copy()
df_ips = df_ips[df_ips['geoRegion'] == 'CH'].copy()


# In[22]:


#get relevant data from dataframes
#logic: keep all rows that are not null and get the value from the last row
inz14 = df_cases[df_cases['inzsumTotal_last14d'].notnull()]['inzsumTotal_last14d'].tail(1).values[0].round(1)
hosp7 = df_hosp[df_hosp['mean7d'].notnull()]['mean7d'].tail(1).values[0].round(1)
re = df_re[df_re['median_R_mean_mean7d'].notnull()]['median_R_mean_mean7d'].tail(1).values[0]
ips = df_ips[df_ips['ICU_Covid19Patients'].notnull()]['ICU_Covid19Patients'].tail(1).values[0].round(1)


# In[6]:


#decide if goal is met (display hook) or not (display cross)
hook = 'https://i.imgur.com/Kv7PPSl.png'
cross = 'https://i.imgur.com/a4fVQrc.png'

if inz14 < 350:
    symbol_inz14 = hook
else:
    symbol_inz14 = cross
    
if hosp7 < 80:
    symbol_hosp7 = hook
else:
    symbol_hosp7 = cross
    
if re < 1.15:
    symbol_re = hook
else:
    symbol_re = cross
    
if ips < 300:
    symbol_ips = hook
else:
    symbol_ips = cross


# In[13]:


#prepare data for dataframe
data = [
    {'Indikator': f'![symbol]({symbol_inz14}) 14-Tages-Inzidenz', 'Aktuell': inz14, 'Richtwert': '350'},
    {'Indikator': f'![symbol]({symbol_re}) R-Wert (Ø 7 Tage)', 'Aktuell': re, 'Richtwert': '1.15'},
    {'Indikator': f'![symbol]({symbol_ips}) IPS-Belegung (Betten)', 'Aktuell': ips, 'Richtwert': '300'},
    {'Indikator': f'![symbol]({symbol_hosp7}) Hospitalisierungen (Ø 7 Tage)', 'Aktuell': hosp7, 'Richtwert': '80'}
]


# In[14]:


#final dataframe
df = pd.DataFrame(data=data)


# In[15]:


#make a backup export of the current data
df.to_csv("/root/covid_aargau/backups/richtwerte/verschärfungen_{}.csv".format(backdate(0)), index=False)

#export to csv
df.to_csv("/root/covid_aargau/data/richtwerte/verschärfungen.csv", index=False)


# **Datawrapper date update**

# In[45]:


#get the date of the values and today's date
inz14_date = pd.to_datetime(df_cases[df_cases['inzsumTotal_last14d'].notnull()]['datum'].tail(1).values[0])
hosp7_date = pd.to_datetime(df_hosp[df_hosp['mean7d'].notnull()]['datum'].tail(1).values[0])
re_date = pd.to_datetime(df_re[df_re['median_R_mean_mean7d'].notnull()]['date'].tail(1).values[0])
ips_date = pd.to_datetime(df_ips[df_ips['ICU_Covid19Patients'].notnull()]['date'].tail(1).values[0])

today = pd.to_datetime(backdate(0))


# In[52]:


#add one to key if data is current
keys = 0

if inz14_date + timedelta(days=1) >= today:
    keys += 1
if hosp7_date + timedelta(days=6) >= today:
    keys +=1
if re_date + timedelta(days=10) >= today:
    keys+= 1
if ips_date + timedelta(days=1) >= today:
    keys += 1


# In[ ]:


def chart_updater(chart_id):

    url_update = url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': f'Stand: {backdate(0)}'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=headers)

#if all four keys are present, update the charts date
if keys == 4:
    chart_updater(chart_id)

