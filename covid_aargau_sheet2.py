#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from general_settings import file_url, backdate
from datetime import date
from time import sleep


# # isolation and quarantine

# In[2]:


#open sheet
#sleep(20)
df_iso = pd.read_excel(file_url, sheet_name="2. Contact Tracing")


# In[3]:


#rename and choose header
df_iso.iloc[1,0] = "date"
df_iso.columns = df_iso.iloc[1]
df_iso = df_iso.drop([0,1])


# In[4]:


#formatting
df_iso["date"] = pd.to_datetime(df_iso["date"], errors="coerce")
df_iso.columns = ["date", "new_isolated", "total_curr_isolated", "total_isolated",
                 "new_quar", "total_curr_quar", "total_quar", "NaN"]

#get rid of last row
df_iso.drop(df_iso.tail(1).index,inplace=True)

#if Monday (weekday == 0), take Friday as latest values
if date.today().weekday() == 0:
    df_iso = df_iso[df_iso["date"] < backdate(2)]
else:
    df_iso = df_iso[df_iso["date"] < backdate(0)]


# In[5]:


#fill NaN values with previous value
df_iso = df_iso.fillna(method='ffill')

#get relevant rows and columns
df_iso_time = df_iso[["date", "total_curr_isolated", "total_curr_quar"]].copy()

#formatting
df_iso_time["date"] = df_iso_time["date"].dt.normalize()
df_iso_time.columns = ["Datum", "Isolation", "Quarantäne"]
df_iso_time["Quarantäne"] = df_iso_time["Quarantäne"].replace("n.d.", np.nan)


# In[ ]:


#make a backup export of the current data
df_iso_time.to_csv("/root/covid_aargau/backups/iso_over_time/backup_{}.csv".format(backdate(0)))

#export to csv
df_iso_time.to_csv("/root/covid_aargau/data/only_AG/iso_over_time.csv", index=False)

