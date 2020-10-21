#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
from datetime import timedelta
import general_settings
from time import sleep


# # latest daily numbers

# In[2]:


#read xlsx-file from Aargauer Kantonswebsite, cleaning
sleep(5)
df = pd.read_excel(general_settings.file_url, sheet_name=1)

#renaming, choosing headers
df.iloc[1,0] = "date"
df.iloc[1,13] = "neue_todesfälle"
df.iloc[1,14] = "todesfälle_gesamt"
df.columns = df.iloc[1]
df = df.drop([0,1], axis=0).reset_index()

#choose relevant columns
df2 = df.iloc[:, :16]
df2 = df2[df2.date != "Summe"]
df2["date"] = pd.to_datetime(df2["date"], errors="coerce")


# In[3]:


#calculate 7 day rolling average
df2["date_diff"] = df2.date.diff(periods=7)
df2["7_d_mean"] = df2["Neue Fälle"].rolling(7).mean().round(1)
df2.loc[df2["date_diff"] != "7 days", "7_d_rolling"] = -1
df2.loc[df2["date_diff"] == "7 days", "7_d_rolling"] = df2["7_d_mean"]
df2["date"] = pd.to_datetime(df2.date).dt.normalize()

#calculate 3 day rolling average
df2["3_d_rolling"] = df2["Neue Fälle"].rolling(3).sum()


# In[4]:


#take relevant columns to new dataframe
df_cases = df2[["date",
               "Neue Fälle",
               "Gesamtzahl",
               "7_d_rolling",
                "3_d_rolling",
               "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)",
              "neue_todesfälle",
              "todesfälle_gesamt"]]
df_cases = df_cases.drop(df_cases.tail(1).index)


# In[5]:


#formatting
a = "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)"
df_cases[a] = df_cases[a].astype(float)
df_cases = df_cases.round(1)


# In[6]:


#append weekday to calculate if it is monday
df_cases["weekday"] = df_cases["date"].dt.weekday


# In[7]:


#make Series with latest numbers
s_final = df_cases[df_cases["Neue Fälle"] >= 0].iloc[-1]


# In[8]:


#get numbers from same day one week earlier
timestamp_prev_week = s_final["date"] - timedelta(days=7)
timestamp_prev_week = pd.to_datetime(timestamp_prev_week)
df_prev_week = df_cases[df_cases["date"] == timestamp_prev_week]

#concat Series and DataFrame
df_s_final = pd.DataFrame(s_final).T
df_final = pd.concat([df_prev_week, df_s_final]).reset_index()

#rename columns
df_final.columns = ["index",
                    "date",
                    "Fälle neu",
                    "Fälle total",
                    "Fall-Durchschnitt letzte 7 Tage",
                    "3_d_rolling",
                    "Fälle pro 100'000 EW pro 14 Tage",
                    "Todesfälle neu",
                    "Todesfälle total",
                    "weekday"]


# In[9]:


#if monday: take 3_d_rolling as "Neue Fälle"
df_final.loc[df_final["weekday"] == 6, "Fälle neu"] = df_final["3_d_rolling"]

#get rid of weekday and 3_d_rolling
df_final = df_final.drop(columns=["weekday", "3_d_rolling"])

#formatting
try:
    df_final["Fälle neu"] = df_final["Fälle neu"].astype(int)
    df_final["Fälle total"] = df_final["Fälle total"].astype(int)
except:
    pass


# In[10]:


#build first df without date (daily numbers)
df_final2 = df_final.loc[:, df_final.columns != "date"].T

#get date of latest numbers, change to string
date_current_values = df_final["date"].loc[1].date().strftime("%d.%m.%Y")
date_current_values = "Daten vom " + date_current_values

#rename columns
df_final2.columns = ["Vorwoche", date_current_values]


# In[11]:


#build second df without date and calculate pct_change over one week (diff between daily numbers)
df_final3 = df_final.loc[:, df_final.columns != "date"].pct_change().multiply(100).round(1)
df_final3 = df_final3.T


# In[12]:


#concat daily numbers and difference
df_final4 = pd.concat([df_final2, df_final3], axis=1)

#drop not needed row and column
df_final4 = df_final4.drop(["index"])
df_final4 = df_final4.drop(columns=[0])


# In[13]:


#reorder columns
col_list = df_final4.columns.tolist()
myorder = [1,0,2]
col_list = [col_list[i] for i in myorder]
df_final4= df_final4[col_list]
df_final4.columns = [date_current_values, "Vorwoche", "+/- in %"]

#fillna in diff column with 0
df_final4["+/- in %"] = df_final4["+/- in %"].fillna(0)
df_final4["+/- in %"] = df_final4["+/- in %"].astype(str) + "%"


# In[14]:


if df_final4.iloc[4,2] == "inf%":
    df_final4.loc[df_final4["+/- in %"] == "inf%", "+/- in %"] = np.nan


# In[15]:


#make a backup export of the current data
df_final4.to_csv("/root/covid_aargau/backups/daily_data/backup_{}.csv".format(general_settings.today))

#export to csv
df_final4.to_csv("/root/covid_aargau/daily_data.csv")


# # daily new cases as line graph

# In[16]:


df_dailys = df_cases
df_dailys2 = df_dailys[df_dailys["Neue Fälle"] > -1]
df_dailys3 = df_dailys2[["date", "Neue Fälle", "7_d_rolling"]]
df_dailys3.columns = ["date", "Neue Fälle", "7-Tages-Durchschnitt"]
df_dailys3.reset_index(drop=True, inplace=True)


# In[17]:


#add a baseline (for visualization purposes in Datawrapper)
df_dailys3["baseline"] = 0


# In[18]:


#replace -1 with NaN
df_dailys3 = df_dailys3.replace(-1, np.nan)


# In[19]:


#make a backup export of the current data
df_dailys3.to_csv("/root/covid_aargau/backups/daily_over_time/backup_{}.csv".format(general_settings.today))

#export to csv
df_dailys3.to_csv("/root/covid_aargau/daily_over_time.csv", index=False)


# # hospital numbers

# In[20]:


df_hosp = df2[["date", "Bestätigte Fälle ohne IPS/IMC", "Bestätigte Fälle IPS/IMC", "Restkapazität Betten IPS/IMC"]]

#if Monday (weekday == 0), take Friday as latest values
if general_settings.todays_weekday == 0:
    df_hosp2 = df_hosp[df_hosp["date"] < general_settings.two_days_ago]
else:
    df_hosp2 = df_hosp[df_hosp["date"] < general_settings.today]


# In[21]:


df_hosp2 = df_hosp2.replace(-1, np.nan)
df_hosp2 = df_hosp2.fillna(method='ffill')
df_hosp2.columns = ["Datum",
                    "Hosp. ohne Intensivpflege",
                     "Hosp. mit Intensivpflege",
                     "Restkapazität Intensiv-Betten"]

#make a backup export of the current data
df_hosp2.to_csv("/root/covid_aargau/backups/hosp_numbers/backup_{}.csv".format(general_settings.today))

#export to csv
df_hosp2.to_csv("/root/covid_aargau/hosp_numbers.csv", index=False)

