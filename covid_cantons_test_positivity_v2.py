#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
from time import sleep
from general_settings import backdate
from datetime import datetime, timedelta


# In[ ]:


base_url = "https://www.covid19.admin.ch/api/data/context"


# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["daily"]["test"]
df_import = pd.read_csv(url1)


# In[ ]:


dfch = pd.DataFrame([])


# In[ ]:


def test_pos(canton):
    dfc = df_import[df_import["geoRegion"] == canton].copy()
    dfc["datum"] = pd.to_datetime(dfc["datum"])
    
    dfc2 = dfc[dfc["datum"] >= pd.to_datetime("2020-08-31")]
    dfc2.set_index("datum", inplace=True)
    
    dfc3 = dfc2.resample("W")["entries_pos", "entries_neg"].sum()
    
    dfc4 = dfc3[dfc3.index < datetime.today()].copy()
    dfc4.index = dfc4.index - timedelta(days=6)
    dfc4.columns = ["positiv", "negativ"]
    
    dfc4["Positivitätsrate_{}".format(canton)] = ((dfc4["positiv"] / (dfc4["positiv"] + dfc4["negativ"])) * 100).round(1)
    if canton != "CH":
        #export to csv
        dfc4[["positiv", "negativ"]].to_csv("/root/covid_aargau/data/tests_weekly/tests_weekly_{}.csv".format(canton))
    
    if canton == "CH":
        global dfch
        dfch = pd.concat([dfc4[["Positivitätsrate_{}".format(canton)]], dfch])
    else:
        dfc_final = dfc4[["Positivitätsrate_{}".format(canton)]].merge(dfch, left_index=True, right_index=True)
        dfc_final.columns = [canton, "Schweiz"]
        
        #export to csv
        dfc_final.to_csv("/root/covid_aargau/data/positivity_weekly/positivity_weekly_{}.csv".format(canton))


# In[ ]:


cantons = ["CH", "AG", "SO", "SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR"]

for canton in cantons:
    test_pos(canton)
    sleep(5)

