#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
import general_settings
from time import sleep


# # venues of infection

# In[2]:


#open sheet nr. 3
sleep(35)
df_ven = pd.read_excel(general_settings.file_url, sheet_name="3. Ansteckungsorte")


# In[3]:


#rename and choose header
df_ven.iloc[0,0] = "date"
df_ven.columns = df_ven.iloc[0]
df_ven["date"] = pd.to_datetime(df_ven["date"], errors="coerce")

#get only columns that are not named NaN
df_ven = df_ven.loc[:, df_ven.columns.notnull()]

#change NaN values temporarily to -1
df_ven = df_ven.fillna(-1)


# In[4]:


#drop rows containing text
indexes = df_ven.loc[(df_ven["Unbekannt"] == "Unbekannt") |
                     (df_ven["Geschäfte und Läden"] == "Prüfsumme:") |
                    (df_ven["Unbekannt"] == "Neue Fälle")].index
df_ven = df_ven.drop(indexes, axis=0).reset_index()

#column "Unbekannt" to integer
df_ven["Unbekannt"] = df_ven["Unbekannt"].astype(int)


# In[5]:


#get date with latest values
#take date where "Unbekannt" is bigger than -1, take second to last row...
#...(because last row contains date: -1, Unbekannt: 831 (current total of Unbekannt))
current_date = df_ven["date"][df_ven["Unbekannt"] > -1].iloc[-2]
current_date = current_date.date().strftime("%d.%m.%Y")


# In[6]:


#build Series with latest values
s_ven = df_ven[df_ven["Unbekannt"] > -1].iloc[-1]
s_ven = s_ven.drop(["index", "date"])

#rebuild DataFrame
df_ven_final = pd.DataFrame(s_ven)

#formatting
df_ven_final.columns = ["total"]


# In[7]:


#calculate sum
sum_cases = df_ven_final["total"].sum()

#calculate percentages
df_ven_final["in Prozent"] = ((df_ven_final["total"].astype(int) / sum_cases)*100).round(1)

#get new index
df_ven_final = df_ven_final.reset_index()

#rename column
df_ven_final = df_ven_final.rename(columns={0: "Stand: {}".format(current_date)})


# In[8]:


#make a backup export of the current data
df_ven_final.to_csv("/root/covid_aargau/backups/venues/backup_{}.csv".format(general_settings.today))

#export to csv
df_ven_final.to_csv("/root/covid_aargau/venues_counter.csv", index=False)

