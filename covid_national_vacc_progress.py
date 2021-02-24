#!/usr/bin/env python
# coding: utf-8

# # Impf-Fortschritt gesamtschweizerisch

# In[1]:


import pandas as pd
import requests
from time import sleep
import numpy as np
from general_settings import backdate


# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': 'Bearer exBDzRsC86QAktkFECOOvK0ZjVTDN2u1LOWq6VjdTsaHUh9mjaKJodeYRIh75F68'}
chart_id = '8hVAj'


# In[3]:


def vaccinator(vacc_status, filename):
    
    #get BAG data, vaccines that were already given to people (df_import1) and fully vacc. people (df_import2)
    r = requests.get(base_url)
    response = r.json()
    files = response['sources']['individual']
    url = files['csv'][filename]
    df_import = pd.read_csv(url)
    
    #choose only CH
    df = df_import[df_import['geoRegion'] == 'CH'].copy()
    
    #formatting, choose only last row
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by='date', inplace=True)
    df_latest = df.tail(1).copy()
    df_latest['date'] = df_latest['date'].dt.strftime('%d.%m.%Y')

    #extract latest vacc data and date
    vacc = df_latest['sumTotal'].values[0]
    pop = df_latest['pop'].values[0]
    last_updated = df_latest['date'].values[0]
    
    #stuff data into a dataframe, return it
    temp_dict = {'vacc_status': vacc_status, 'date': last_updated, 'sum': vacc, 'pop': pop}
    df_return = pd.DataFrame([temp_dict])
    
    return df_return


# In[4]:


#files to call the function on
files = {'all vacc given': 'vaccDosesAdministered', 'people two vacc': 'fullyVaccPersons'}

#call function for each file, concat Dataframes to df_final
df_final = pd.DataFrame([])

for key, value in files.items():
    df_temp = vaccinator(key, value)
    df_final = pd.concat([df_final, df_temp])
    sleep(3)
    
df_final.reset_index(inplace=True, drop=True)


# In[ ]:


#proceed only if the two dates match
if df_final['date'].nunique() == 1:
    
    #total population
    pop_total = df_final['pop'].loc[0]
    
    #how many people got two shots
    people_two_vacc = df_final['sum'].loc[1]
    people_two_vacc_rel = ((people_two_vacc / pop_total) * 100).round(1)
    
    #how many people got one shot? Total vacc. administered minus people who are fully vacc. * 2 (they got 2 shots)
    people_one_vacc = (df_final['sum'].loc[0] - (people_two_vacc * 2))
    people_one_vacc_rel = ((people_one_vacc / pop_total) * 100).round(1)
    
    #not vaccined
    people_no_vacc = pop_total - people_one_vacc - people_two_vacc
    people_no_vacc_rel = (((pop_total - people_one_vacc - people_two_vacc) / pop_total) * 100).round(1)
    
    #last update
    last_updated = df_final['date'].loc[0]
    
    #build Dataframe with latest vaccine data
    vacc_data = {
        'Beschreibung': ['Impf-Fortschritt'],
        'vollständig geimpft': [people_two_vacc_rel],
        'einfach geimpft': [people_one_vacc_rel],
        'ungeimpft': [people_no_vacc_rel]
    }

    df_vacc = pd.DataFrame(data=vacc_data)
    
    note = f'<span style="color:#07850d">{people_two_vacc_rel} Prozent ist zweifach geimpft</span>, <span style="color:#51c14b">{people_one_vacc_rel} Prozent einfach</span> und <span style="color:#808080">{people_no_vacc_rel} Prozent ungeimpft</span>'
    
    #function chart_updater updates only the date in the annotation section and re-publishes the chart
    def chart_updater(chart_id, date):

        url_update = url + chart_id
        url_publish = url_update + '/publish'

        payload = {

        'metadata': {'annotate': {'notes': f'{note}. Stand: {str(date)}.'}}

        }

        res_update = requests.patch(url_update, json=payload, headers=headers)

        sleep(3)

        res_publish = requests.post(url_publish, headers=headers)

    #call function
    chart_updater(chart_id, last_updated)
    
    
else:
    pass


# In[ ]:


#make a backup export of the current data
df_vacc.to_csv("/root/covid_aargau/backups/vacc_ch/backup_CH_{}.csv".format(backdate(0)), index=False)

#export to csv
df_vacc.to_csv("/root/covid_aargau/data/vaccination/vacc_ch.csv", index=False)

