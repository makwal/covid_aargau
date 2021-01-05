#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from general_settings import file_url, backdate
from datetime import date
from time import sleep


# In[2]:


#open sheet
sleep(50)
df_travel = pd.read_excel(file_url, sheet_name="5. Quarantäne nach Einreise")


# In[3]:


#rename and choose header
df_travel.iloc[1,0] = "date"
df_travel.columns = df_travel.iloc[1]
df_travel = df_travel.drop([0,1])

#remove rows with NaN in col header
df_travel = df_travel.loc[:, df_travel.columns.notnull()]

#formatting
df_travel["date"] = pd.to_datetime(df_travel["date"], errors="coerce").dt.normalize()
df_travel.columns = ["date", "Fälle neu", "aktuell betreut", "Fälle total"]


# In[4]:


#if Monday (weekday == 0), take Friday as latest values
if date.today().weekday() == 0:
    df_travel = df_travel[df_travel["date"] < backdate(2)]
else:
    df_travel = df_travel[df_travel["date"] < backdate(0)]


# In[5]:


#fill NaN values with previous value
df_travel = df_travel.fillna(method='ffill')

#choose relevant columns
df_travel = df_travel[["date", "aktuell betreut"]]


# In[6]:


#replace "n.d." no data with NaN
df_travel.loc[df_travel["aktuell betreut"] == "n.d.", "aktuell betreut"] = np.nan


# In[ ]:


#make a backup export of the current data
df_travel.to_csv("/root/covid_aargau/backups/travel/backup_{}.csv".format(backdate(0)))

#export to csv
df_travel.to_csv("/root/covid_aargau/data/only_AG/travel.csv", index=False)

