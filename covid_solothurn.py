#!/usr/bin/env python
# coding: utf-8

# In[44]:


import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate
from time import sleep
import requests


# In[45]:


base_url = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv_v2/COVID19_Fallzahlen_Kanton_SO_total.csv"
df_import = pd.read_csv(base_url)


# In[46]:


df = df_import[["date", "ncumul_conf", "current_hosp", "current_icu", "ncumul_deceased"]].copy()
df["date"] = pd.to_datetime(df["date"])

#fill NaN values with previous value
df = df.ffill()


# In[47]:


#calculate daily data
df["cases"] = df["ncumul_conf"].diff()
df["new_hosp"] = df["current_hosp"].diff()
df["new_icu"] = df["current_icu"].diff()
df["new_dec"] = df["ncumul_deceased"].diff()


# In[48]:


#get last row and same day from the previous week
today = df.tail(1)
last_week = df[df["date"] == today.iloc[-1]["date"] - timedelta(days=7)]


# In[49]:


#unite both rows in df, rename columns
df2 = pd.concat([today, last_week])
df2.columns = ["Datum", "Fälle gesamt", "hospitalisierte Patienten", "davon auf der Intensiv-Station", "Todesfälle gesamt", "Neue Fälle", "Veränderung Spital-Belegung", "Veränderung Intensiv-Belegung", "Todesfälle neu"]


# In[50]:


#reorder columns
col_list = df2.columns.tolist()
myorder = [0,5,1,8,4,2,3,6,7]
col_list = [col_list[i] for i in myorder]
df3= df2[col_list]

#get date for "Zahlen vom..."
date_current_values = df3.iloc[0]["Datum"].date().strftime("%d.%m.%Y")
date_current_values = "Zahlen vom " + date_current_values

#set date as index
df3.set_index("Datum", inplace=True)

#all cols to int
df3 = df3.astype(int)


# In[51]:


#hospital changes in ^^ (for datawrapper styling reasons)
df3.loc[df3["Veränderung Spital-Belegung"] > 0, "Veränderung Spital-Belegung"] = "+" + df3["Veränderung Spital-Belegung"].astype(str)
df3.loc[df3["Veränderung Intensiv-Belegung"] > 0, "Veränderung Intensiv-Belegung"] = "+" + df3["Veränderung Intensiv-Belegung"].astype(str)

df3["hospitalisierte Patienten"] = df3["hospitalisierte Patienten"].astype(str) + "^" + df3["Veränderung Spital-Belegung"].astype(str) + "^"
df3["davon auf der Intensiv-Station"] = df3["davon auf der Intensiv-Station"].astype(str) + "^" + df3["Veränderung Intensiv-Belegung"].astype(str) + "^"


# In[53]:


df3.rename(columns={
    "hospitalisierte Patienten": "hospitalisierte Patienten ^seit Vortag^",
    "davon auf der Intensiv-Station": "davon auf der Intensiv-Station ^seit Vortag^"
}, inplace=True)

#transpose
df_final = df3[["Neue Fälle", "Fälle gesamt", "Todesfälle neu", "Todesfälle gesamt", "hospitalisierte Patienten ^seit Vortag^", "davon auf der Intensiv-Station ^seit Vortag^"]].T
df_final.columns = [date_current_values, "vor einer Woche"]


# In[ ]:


#make a backup export of the current data
df_final.to_csv("/root/covid_aargau/backups/daily_data_SO/backup_{}.csv".format(backdate(0)))

#export to csv
df_final.to_csv("/root/covid_aargau/data/only_AG/daily_data_SO.csv")

