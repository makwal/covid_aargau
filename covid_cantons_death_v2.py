#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#in Betrieb ab 9. 12. 2020
import pandas as pd
import requests
from time import sleep
from general_settings import backdate
from datetime import timedelta


# In[ ]:


base_url = "https://www.covid19.admin.ch/api/data/context"


# In[ ]:


r = requests.get(base_url)
response = r.json()
files = response["sources"]["individual"]
url1 = files["csv"]["weekly"]["byAge"]["death"]
df_import = pd.read_csv(url1)


# In[ ]:


def death(canton):
    #get canton
    df = df_import[(df_import["geoRegion"] == canton)]
    
    #make list of all age categories
    age_list = df["altersklasse_covid19"].unique()

    
    #get last row of each age category, concat to df_final
    df_final = pd.DataFrame([])

    for a in age_list:
        row = df[df["altersklasse_covid19"] == a].tail(1)[["altersklasse_covid19", "sumTotal"]]
        df_final = pd.concat([df_final, row])

    df_final.columns = ["Altersklasse", "Todesfälle"]
        
    #export backup to csv
    df_final.to_csv("/root/covid_aargau/backups/death/death_{}_{}.csv".format(canton, backdate(0)), index=False)
    
    #export to csv
    df_final.to_csv("/root/covid_aargau/data/death_{}.csv".format(canton), index=False)


# In[ ]:


cantons = ["AG", "SG", "AI", "AR", "TG", "LU", "ZG", "SZ", "OW", "NW", "UR"]

for canton in cantons:
    death(canton)
    sleep(5)

