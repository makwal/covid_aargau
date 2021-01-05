#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate
from datetime import timedelta


# In[11]:


base_url = "https://www.covid19.admin.ch/api/data/context"


# In[12]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["daily"]["cases"]
df_import = pd.read_csv(url1)


# In[9]:


def daily_cases(canton):    
    
    #get canton and relevant columns
    df = df_import[(df_import["geoRegion"] == canton)][["datum", "entries", "mean7d"]]
    df.columns = ["Datum", "Fälle", "7-Tages-Durchschnitt"]
    
    #add a baseline (for visualization purposes in Datawrapper)
    df["baseline"] = 0

    #export backup to csv
    df.to_csv("/root/covid_aargau/backups/daily_cases/daily_cases_{}_{}.csv".format(canton, backdate(0)), index=False)
    
    #export to csv
    df.to_csv("/root/covid_aargau/data/daily_cases/daily_cases_{}.csv".format(canton), index=False)


# In[ ]:


cantons = ["SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR"]

for canton in cantons:
    daily_cases(canton)
    sleep(5)

