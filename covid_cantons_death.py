#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd
import general_settings


# In[39]:


#get data from github
df = pd.read_csv("https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid19_cases_fatalities_switzerland_bag.csv")

#formatting
df["date_new"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
df.pop("date")
df.set_index("date_new", inplace=True)
df.rename_axis(None, inplace=True)


# In[40]:


#choose AG
df_ag = df[df["canton"] == "AG"]
df_ag2 = df_ag.drop(["canton", "cases"], axis=1)


# In[46]:


#groupby age
s = df_ag2.groupby("age_group")["fatalities"].sum()
df_final = pd.DataFrame(s)
df_final.index.name = "Altersgruppe"
df_final.columns = ["Todesfälle"]


# In[49]:


#export backup to csv
df_final.to_csv("/root/covid_aargau/backups/death/todesfaelle_AG_{}.csv".format(general_settings.today))

#export to csv
df_final.to_csv("/root/covid_aargau/data/deaths.csv")

