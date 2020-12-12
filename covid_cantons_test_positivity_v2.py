#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate
from datetime import datetime, timedelta


# In[2]:


base_url = "https://www.covid19.admin.ch/api/data/context"


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["daily"]["test"]
df_import = pd.read_csv(url1)


# In[4]:


dfch = pd.DataFrame([])


# In[5]:


def test_pos(canton):
    dfc = df_import[df_import["geoRegion"] == canton]
    dfc["datum"] = pd.to_datetime(dfc["datum"])
    
    dfc2 = dfc[dfc["datum"] >= pd.to_datetime("2020-06-15")]
    dfc2.set_index("datum", inplace=True)
    
    dfc3 = dfc2.resample("W")["entries_pos", "entries_neg"].sum()
    
    dfc4 = dfc3[dfc3.index < datetime.today()]
    dfc4.index = dfc4.index - timedelta(days=6)
    dfc4.columns = ["positiv", "negativ"]
    
    dfc4["Positivitätsrate_{}".format(canton)] = ((dfc4["positiv"] / (dfc4["positiv"] + dfc4["negativ"])) * 100).round(1)
    if canton != "CH":
        #make a backup export of the current data
        dfc4[["positiv", "negativ"]].T.to_csv("/root/covid_aargau/backups/tests/tests_weekly_{}_{}.csv".format(canton, backdate(0)))
        
        #export to csv
        dfc4[["positiv", "negativ"]].T.to_csv("/root/covid_aargau/data/tests_weekly_{}.csv".format(canton))
    
    if canton == "CH":
        global dfch
        dfch = pd.concat([dfc4[["Positivitätsrate_{}".format(canton)]], dfch])
    else:
        dfc_final = dfc4[["Positivitätsrate_{}".format(canton)]].merge(dfch, left_index=True, right_index=True)
        dfc_final.columns = [canton, "Schweiz"]
        
        #make a backup export of the current data
        dfc_final.to_csv("/root/covid_aargau/backups/positivity/positivity_weekly_{}_{}.csv".format(canton, backdate(0)))
        
        #export to csv
        dfc_final.to_csv("/root/covid_aargau/data/positivity_weekly_{}.csv".format(canton), index=False)


# In[6]:


cantons = ["CH", "AG", "SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR"]

for canton in cantons:
    test_pos(canton)
    sleep(5)


# In[ ]:




