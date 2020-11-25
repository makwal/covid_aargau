#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import timedelta
import general_settings
from time import sleep


# # latest daily numbers

# In[2]:


#read xlsx-file from Aargauer Kantonswebsite, cleaning
df = pd.read_excel(general_settings.file_url, sheet_name="1. Covid-19-Daten")

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


#calculate 7 day rolling average (- 3 days)
df2["hilfs"] = df2["Neue Fälle"][df2["date"] < pd.to_datetime(general_settings.two_days_ago) - timedelta(days=1)]
df2["7_d_rolling"] = df2["hilfs"].rolling(7).mean().round(0)

df2["date"] = pd.to_datetime(df2.date).dt.normalize()

#calculate 3 day rolling average (only used for weekend)
df2["3_d_rolling"] = df2["Neue Fälle"].rolling(3).sum()
df2["3_d_rolling_deaths"] = df2["neue_todesfälle"].rolling(3).sum()


# In[4]:


#take relevant columns to new dataframe
df_cases = df2[["date",
               "Neue Fälle",
               "Gesamtzahl",
               "7_d_rolling",
                "3_d_rolling",
               "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)",
              "neue_todesfälle",
              "todesfälle_gesamt",
               "3_d_rolling_deaths"]]
df_cases = df_cases.drop(df_cases.tail(1).index)


# In[5]:


#formatting
a = "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)"
df_cases[a] = df_cases[a].astype(float)
df_cases = df_cases.round(0)


# In[6]:


#append weekday to calculate if it is monday
df_cases["weekday"] = df_cases["date"].dt.weekday


# In[7]:


#make Series with latest numbers
s_final = df_cases[df_cases["Neue Fälle"] >= 0].iloc[-1]

#add 7 day rolling average to Series
rolling7 = df_cases["7_d_rolling"][df_cases["7_d_rolling"] >= 0].iloc[-1]
s_final[3] = rolling7


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
                    "7-Tages-Durchschnitt",
                    "3_d_rolling",
                    "Fälle pro 100'000 EW pro 14 Tage",
                    "Todesfälle neu",
                    "Todesfälle total",
                    "3_d_rolling_deaths",
                    "weekday"]


# In[9]:


#if monday: take 3_d_rolling as "Neue Fälle"
df_final.loc[df_final["weekday"] == 6, "Fälle neu"] = df_final["3_d_rolling"]
df_final.loc[df_final["weekday"] == 6, "Todesfälle neu"] = df_final["3_d_rolling_deaths"]

#get rid of weekday and 3_d_rolling
df_final = df_final.drop(columns=["weekday", "3_d_rolling", "3_d_rolling_deaths"])

#formatting
try:
    df_final["Fälle neu"] = df_final["Fälle neu"].astype(int)
    df_final["Fälle total"] = df_final["Fälle total"].astype(int)
except:
    pass


# In[28]:


#read in yesterday's backup
url_yest = "https://raw.githubusercontent.com/makwal/covid_aargau/master/backups/daily_data/backup_{}.csv"
dfe = pd.read_csv(url_yest.format(general_settings.yesterday))

#"Zahlen vom " day before yesterday
day_b_y = dfe.columns[1]

#calculate Nachmeldungen Fälle
cases_total_yest = dfe[day_b_y].iloc[1]
cases_total_today = df_final["Fälle total"].tail(1)
cases_new_today = df_final["Fälle neu"].tail(1)
nachmeldungen_cases = int(cases_total_today) - int(cases_total_yest) - int(cases_new_today)

#calculate Nachmeldungen Todesfälle
death_total_yest = dfe[day_b_y].iloc[5]
death_total_today = df_final["Todesfälle total"].tail(1)
death_new_today = df_final["Todesfälle neu"].tail(1)
nachmeldungen_tod = int(death_total_today) - int(death_total_yest) - int(death_new_today)

#append to df
df_final["Nachmeldungen Fälle"] = [np.nan, nachmeldungen_cases]
df_final["Nachmeldungen Todesfälle"] = [np.nan, nachmeldungen_tod]


# In[11]:


#build first df without date (daily numbers)
df_final2 = df_final.loc[:, df_final.columns != "date"].T

#get date of latest numbers, change to string
date_current_values = df_final["date"].loc[1].date().strftime("%d.%m.%Y")
date_current_values = "Zahlen vom " + date_current_values

#rename columns
df_final2.columns = ["vor einer Woche", date_current_values]


# In[12]:


#build second df without date and calculate pct_change over one week (diff between daily numbers)
df_final3 = df_final.loc[:, df_final.columns != "date"].pct_change().multiply(100).round(1)
df_final3 = df_final3.T


# In[13]:


#concat daily numbers and difference
df_final4 = pd.concat([df_final2, df_final3], axis=1)

#drop not needed row and column
df_final4 = df_final4.drop(["index"])
df_final4 = df_final4.drop(columns=[0])


# In[14]:


#reorder columns
col_list = df_final4.columns.tolist()
myorder = [1,0,2]
col_list = [col_list[i] for i in myorder]
df_final4= df_final4[col_list]
df_final4.columns = [date_current_values, "vor einer Woche", "+/- in %"]

#fillna in diff column with 0
df_final4["+/- in %"] = df_final4["+/- in %"].fillna(0)
df_final4["+/- in %"] = df_final4["+/- in %"].astype(str) + "%"


# In[15]:


if df_final4.iloc[4,2] == "inf%":
    df_final4.loc[df_final4["+/- in %"] == "inf%", "+/- in %"] = np.nan
    
df_final4.iloc[6,2] = np.nan
df_final4.iloc[7,2] = np.nan


# In[16]:


df_final4


# In[ ]:


#make a backup export of the current data
df_final4.to_csv("/root/covid_aargau/backups/daily_data/backup_{}.csv".format(general_settings.today))

#export to csv
df_final4.to_csv("/root/covid_aargau/data/daily_data.csv")


# # daily new cases as line graph

# In[ ]:


df_dailys = df_cases
df_dailys2 = df_dailys[df_dailys["Neue Fälle"] > -1]
df_dailys3 = df_dailys2[["date", "Neue Fälle", "7_d_rolling"]].copy()
df_dailys3.columns = ["date", "Neue Fälle", "7-Tages-Durchschnitt"]
df_dailys3.reset_index(drop=True, inplace=True)


# In[ ]:


#add a baseline (for visualization purposes in Datawrapper)
df_dailys3["baseline"] = 0


# In[ ]:


#replace -1 with NaN
df_dailys3 = df_dailys3.replace(-1, np.nan)


# In[ ]:


#make a backup export of the current data
df_dailys3.to_csv("/root/covid_aargau/backups/daily_over_time/backup_{}.csv".format(general_settings.today))

#export to csv
df_dailys3.to_csv("/root/covid_aargau/data/daily_over_time.csv", index=False)


# # hospital numbers

# In[ ]:


df_hosp = df2[["date", "Bestätigte Fälle ohne IPS/IMC", "Bestätigte Fälle IPS/IMC", "Restkapazität Betten IPS/IMC"]]
df_hosp.set_index("date", inplace=True)
df_hosp["2020-10":].replace(0,-1, inplace=True)
df_hosp.reset_index(inplace=True)


# In[ ]:


#if Monday (weekday == 0), take Friday as latest values
if general_settings.todays_weekday == 0:
    df_hosp2 = df_hosp[df_hosp["date"] < general_settings.two_days_ago]
else:
    df_hosp2 = df_hosp[df_hosp["date"] < general_settings.today]


# In[ ]:


df_hosp2 = df_hosp2.replace(-1, np.nan)
df_hosp2 = df_hosp2.fillna(method='ffill')
df_hosp2.columns = ["Datum",
                    "Hosp. ohne Intensivpflege",
                     "Hosp. mit Intensivpflege",
                     "Restkapazität Intensiv-Betten"]


# In[ ]:


#make a backup export of the current data
df_hosp2.to_csv("/root/covid_aargau/backups/hosp_numbers/backup_{}.csv".format(general_settings.today))

#export to csv
df_hosp2.to_csv("/root/covid_aargau/data/hosp_numbers.csv", index=False)

