#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd
import numpy as np
import general_settings


# In[21]:


#open sheet nr. 8
df_travel = pd.read_excel(general_settings.file_url, sheet_name=8)


# In[22]:


#rename and choose header
df_travel.iloc[1,0] = "date"
df_travel.columns = df_travel.iloc[1]
df_travel = df_travel.drop([0,1])

#formatting
df_travel["date"] = pd.to_datetime(df_travel["date"], errors="coerce").dt.normalize()
df_travel.columns = ["date", "Fälle neu", "aktuell betreut", "Fälle total"]


# In[23]:


#if Monday (weekday == 0), take Friday as latest values
if general_settings.todays_weekday == 0:
    df_travel = df_travel[df_travel["date"] < general_settings.two_days_ago]
else:
    df_travel = df_travel[df_travel["date"] < general_settings.today]


# In[24]:


#fill NaN values with previous value
df_travel = df_travel.fillna(method='ffill')

#choose relevant columns
df_travel = df_travel[["date", "aktuell betreut"]]


# In[25]:


#make a backup export of the current data
df_travel.to_csv("backups/travel/backup_{}.csv".format(general_settings.today))

#export to csv
df_travel.to_csv("travel.csv", index=False)

