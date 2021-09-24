#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate, datawrapper_api_key
from datetime import datetime, timedelta


# In[ ]:


base_url = "https://www.covid19.admin.ch/api/data/context"

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["weekly"]["byAge"]["cases"]
df_import = pd.read_csv(url1)


# In[ ]:


def age_dist_cantons(canton):
    #get canton
    df = df_import[(df_import["geoRegion"] == canton)]
    
    #make list of all age categories
    age_list = df["altersklasse_covid19"].unique()

    
    #get last row of each age category, concat to df_final
    df_final = pd.DataFrame([])
    
    for a in age_list:
        row = df[df["altersklasse_covid19"] == a].tail(1)[["altersklasse_covid19", "sumTotal"]]
        df_final = pd.concat([df_final, row])
    
    #formatting
    df_final.columns = ["Altersklasse", "Covid-Fälle"]
        
    #export to csv
    df_final.to_csv("/root/covid_aargau/data/age/altersverteilung_{}.csv".format(canton), index=False)


# In[ ]:


def chart_updater(chart_id):

    url_publish = datawrapper_url + chart_id + '/publish'
  
    res_publish = requests.post(url_publish, headers=headers)


# In[ ]:


cantons = {"AG": "kOuHT", "SO": "r1M5h", "SG": "dKj8R", "LU": "IAlty", "TG": "3t0xX"}

#create Boolean if datawrapper charts are to be updated
update_day = datetime.today().day % 9 == 0

for canton, chart_id in cantons.items():
    age_dist_cantons(canton)
    if update_day:
        chart_updater(chart_id)
    sleep(5)

