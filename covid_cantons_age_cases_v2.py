#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate
from datetime import timedelta


# In[2]:


base_url = "https://www.covid19.admin.ch/api/data/context"


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
        
    #export backup to csv
    df_final.to_csv("/root/covid_aargau/backups/age/altersverteilung_{}_{}.csv".format(canton, backdate(0)), index=False)
    
    #export to csv
    df_final.to_csv("/root/covid_aargau/data/age/altersverteilung_{}.csv".format(canton), index=False)


# In[ ]:


cantons = ["AG", "SO", "SG", "LU", "TG"]

for canton in cantons:
    age_dist_cantons(canton)
    sleep(5)

