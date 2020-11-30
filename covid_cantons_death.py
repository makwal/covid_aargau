#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from general_settings import backdate
from time import sleep


# In[2]:


def death_getter(canton):
#get data from github
    df = pd.read_csv("https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid19_cases_fatalities_switzerland_bag.csv")

    #formatting
    df["date_new"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df.pop("date")
    df.set_index("date_new", inplace=True)
    df.rename_axis(None, inplace=True)
    
    #choose canton
    if canton == "OCH":
        df_ag = df[(df["canton"] == "SG") | (df["canton"] == "TG") | (df["canton"] == "AR") | (df["canton"] == "AI")]
    elif canton == "ZCH":
        df_ag = df[(df["canton"] == "LU") | (df["canton"] == "SZ") | (df["canton"] == "UR") | (df["canton"] == "OW")  | (df["canton"] == "NW") | (df["canton"] == "ZG")]
    
    else:
        df_ag = df[df["canton"] == canton]
    df_ag2 = df_ag.drop(["canton", "cases"], axis=1)
    
    #groupby age
    s = df_ag2.groupby("age_group")["fatalities"].sum()
    df_final = pd.DataFrame(s)
    df_final.index.name = "Altersklasse"
    df_final.columns = ["Todesfälle"]
    #export backup to csv
    df_final.to_csv("/root/covid_aargau/backups/death/todesfaelle_{}_{}.csv".format(canton, backdate(0)))

    #export to csv
    df_final.to_csv("/root/covid_aargau/data/deaths_{}.csv".format(canton))


# In[3]:


cantons = ["AG", "OCH", "ZCH"]

for canton in cantons:
    death_getter(canton)
    sleep(5)


# In[ ]:




