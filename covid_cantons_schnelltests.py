#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from time import sleep
import general_settings
from datetime import timedelta


# In[2]:


base_url = "https://www.covid19.admin.ch/api/data/context"


# In[3]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["daily"]["testPcrAntigen"]
df_import = pd.read_csv(url1)


# In[5]:


def antigen(canton):
    
    #get all rows for one canton
    df_anti_c = df_import[df_import["geoRegion"] == canton]
    
    #format date, to index
    df_anti_c["datum_neu"] = pd.to_datetime(df_anti_c["datum"], format="%Y-%m-%d")
    df_anti_c.set_index("datum_neu", inplace=True)
    
    #new column weekday, select only relevant columns
    df_anti_c["weekday"] = df_anti_c.index.weekday
    df_anti_c2 = df_anti_c[["weekday", "entries", "sumTotal", "sum7d", "nachweismethode"]]
    
    #get 7-day-value for all sundays from november onwards
    df_anti_c2_sun = df_anti_c2[(df_anti_c2["weekday"] == 6) & (df_anti_c2.index > pd.to_datetime("2020-11"))]
    df_anti_final = df_anti_c2_sun[["nachweismethode", "sum7d"]]
    df_anti_final["nachweismethode"] = df_anti_final["nachweismethode"].str.replace("Antigen_Schnelltest", "Antigen-Schnelltest")
    df_anti_final["nachweismethode"] = df_anti_final["nachweismethode"].str.replace("PCR", "PCR (herkömmlich)")
    
    #date minus six days, in order to display monday
    df_anti_final.index = df_anti_final.index - timedelta(days=6)
    df_anti_final.index = df_anti_final.index.strftime("%d.%m.%Y")
    
    #pivot to perfection
    df_anti_final2 = df_anti_final.pivot_table(index="nachweismethode", columns=df_anti_final.index, values="sum7d")
    
    #export backup to csv
    df_anti_final2.to_csv("/root/covid_aargau/backups/schnelltests/schnelltests_{}_{}.csv".format(canton, general_settings.today))
    
    #export to csv
    df_anti_final2.to_csv("/root/covid_aargau/data/schnelltests_{}.csv".format(canton))


# In[6]:


cantons = ["AG", "SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR"]

for canton in cantons:
    antigen(canton)
    sleep(5)


# In[ ]:




