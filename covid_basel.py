#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate
from time import sleep
import requests


# In[2]:


cantons = ['BS', 'BL']

def covid_basel(canton):
    base_url = "https://raw.githubusercontent.com/openZH/covid_19/master/fallzahlen_kanton_total_csv_v2/COVID19_Fallzahlen_Kanton_{}_total.csv".format(canton)
    df_import = pd.read_csv(base_url)

    df = df_import[["date", "ncumul_conf", "current_hosp", "current_icu", "ncumul_deceased"]].copy()
    df["date"] = pd.to_datetime(df["date"])

    #calculate daily data
    df["cases"] = df["ncumul_conf"].diff()
    df["new_hosp"] = df["current_hosp"].diff()
    df["new_icu"] = df["current_icu"].diff()
    df["new_dec"] = df["ncumul_deceased"].diff()

    #get last row and same day from the previous week
    today = df[df['cases'].notnull()].tail(1)
    last_week = df[df["date"] == today.iloc[-1]["date"] - timedelta(days=7)]

    #unite both rows in df, rename columns
    df2 = pd.concat([today, last_week])
    df2.columns = ["Datum", "Fälle gesamt", "Infizierte im Spital", "davon auf der Intensiv-Station", "Todesfälle gesamt", "Neue Fälle", "Veränderung Spital-Belegung", "Veränderung Intensiv-Belegung", "Todesfälle neu"]

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

    #transpose
    df_final = df3[["Neue Fälle", "Fälle gesamt", "Todesfälle neu", "Todesfälle gesamt", "Infizierte im Spital", "davon auf der Intensiv-Station"]].T
    df_final.columns = [date_current_values, "vor einer Woche"]

    #make a backup export of the current data
    df_final.to_csv("/root/covid_aargau/backups/daily_data_bz/backup_{}_{}.csv".format(backdate(0), canton))

    #export to csv
    df_final.to_csv("/root/covid_aargau/data/only_AG/daily_data_{}.csv".format(canton))


# In[3]:


for canton in cantons:
    covid_basel(canton)
    sleep(5)

