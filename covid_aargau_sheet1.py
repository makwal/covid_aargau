#!/usr/bin/env python
# coding: utf-8

# In[54]:


import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate
from time import sleep


# In[55]:


#read xlsx-file from Aargauer Kantonswebsite, cleaning
df = pd.read_excel(file_url, sheet_name="1. Covid-19-Daten", skiprows=2)


# In[56]:


df.rename(columns={'Unnamed: 0': 'date'}, inplace=True)


# In[57]:


df = df[df.date != "Summe"].copy()
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["date"] = pd.to_datetime(df.date).dt.normalize()


# In[58]:


df_hosp = df[["date", "Bestätigte Fälle Bettenstation (ohne IPS/IMC)", "Bestätigte Fälle Intensivpflegestation (IPS)", "Restkapazität Betten IPS"]].copy()


# In[59]:


#if Monday (weekday == 0), take Friday as latest values
if date.today().weekday() == 0:
    df_hosp2 = df_hosp[df_hosp["date"] < backdate(2)]
else:
    df_hosp2 = df_hosp[df_hosp["date"] < backdate(0)]


# In[60]:


df_hosp2 = df_hosp2.fillna(method='ffill')
df_hosp2.columns = ["Datum",
                    "Patienten ohne Intensivpflege",
                     "Patienten auf Intensiv- oder Überwachungsstation",
                     "freie Beatmungsplätze"]


# In[ ]:


#make a backup export of the current data
df_hosp2.to_csv("/root/covid_aargau/backups/hosp_numbers/backup_{}.csv".format(backdate(0)))

#export to csv
df_hosp2.to_csv("/root/covid_aargau/data/only_AG/hosp_numbers.csv", index=False)

