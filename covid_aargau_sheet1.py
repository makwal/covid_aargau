#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from general_settings import file_url, backdate
from time import sleep


# # latest daily numbers

# In[2]:


#read xlsx-file from Aargauer Kantonswebsite, cleaning
df = pd.read_excel(file_url, sheet_name="1. Covid-19-Daten")


# In[3]:


#renaming, choosing headers
df.iloc[1,0] = "date"
df.iloc[1,20] = "neue_todesfälle"
df.iloc[1,21] = "todesfälle_gesamt"


# In[4]:


df.columns = df.iloc[1]
df = df.drop([0,1], axis=0).reset_index()


# In[5]:


#choose relevant columns
df = df.iloc[:, :23].copy()
df = df[df.date != "Summe"].copy()
df["date"] = pd.to_datetime(df["date"], errors="coerce")


# In[6]:


df["date"] = pd.to_datetime(df.date).dt.normalize()

#calculate 3 day rolling average (only used for weekend)
df["3_d_rolling"] = df["Neue Fälle"].rolling(3).sum()
df["3_d_rolling_deaths"] = df["neue_todesfälle"].rolling(3).sum()


# In[7]:


#take relevant columns to new dataframe
df_cases = df[["date",
                "Neue Fälle",
               "Gesamtzahl",
               "7-Tages-Durchschnitt neue Fälle (+/- 3 Tage)",
                "3_d_rolling",
               "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)",
              "neue_todesfälle",
              "todesfälle_gesamt",
               "3_d_rolling_deaths"]].copy()
df_cases = df_cases.drop(df_cases.tail(1).index)


# In[8]:


#formatting
a = "14-Tage-Inzidenz\n(Anzahl laborbestätigte Fälle pro 100'000 Einwohner pro 14 Tage)"
df_cases[a] = df_cases[a].astype(float)


# In[9]:


#append weekday to calculate if it is monday
df_cases["weekday"] = df_cases["date"].dt.weekday


# In[10]:


#make Series with latest numbers
s_final = df_cases[df_cases["Neue Fälle"] >= 0].iloc[-1]

#add 7 day rolling average to Series
rolling7 = df_cases["7-Tages-Durchschnitt neue Fälle (+/- 3 Tage)"][df_cases["7-Tages-Durchschnitt neue Fälle (+/- 3 Tage)"] >= 0].iloc[-1]
s_final['7-Tages-Durchschnitt neue Fälle (+/- 3 Tage)'] = rolling7


# In[11]:


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


# In[12]:


#if monday: take 3_d_rolling as "Neue Fälle"
df_final.loc[df_final["weekday"] == 6, "Fälle neu"] = df_final["3_d_rolling"]
df_final.loc[df_final["weekday"] == 6, "Todesfälle neu"] = df_final["3_d_rolling_deaths"]

#get rid of weekday and 3_d_rolling
df_final = df_final.drop(columns=["weekday", "3_d_rolling", "3_d_rolling_deaths"])

#formatting
#df_final["Fälle neu"] = df_final["Fälle neu"].astype(int)
#df_final["Fälle total"] = df_final["Fälle total"].astype(int)


# In[13]:


#get Nachmeldungen Fälle and Todesfälle
url_yest = "https://raw.githubusercontent.com/makwal/covid_aargau/master/backups/daily_data/backup_{}.csv"

#check if data is updated (updated == yesterday's date)
date_in_df = df_final.iloc[1]["date"]
date_in_df_prev = df_final.iloc[0]["date"]
data_updated = date_in_df == date.today() - timedelta(days=1)

#if data has been updated today...
if data_updated:
    #... and it's monday...
    if date.today().weekday() == 0:
        #... get dfe(arly) from 3 days ago
        dfe = pd.read_csv(url_yest.format(backdate(3)))
    else:
        # or from yesterday on any other day than monday
        dfe = pd.read_csv(url_yest.format(backdate(1)))

#if data hasn't been updated today...
else:
    #... and it's monday...
    if date.today().weekday() == 0:
        #get dfe(arly) from 4 days ago
        dfe = pd.read_csv(url_yest.format(backdate(4)))
    else:
        #get dfe(arly) from 2 days ago
        dfe = pd.read_csv(url_yest.format(backdate(2)))
    
#"Zahlen vom " day before yesterday
day_b_y = dfe.columns[1]

#calculate Nachmeldungen Fälle
dfe.set_index('Unnamed: 0', inplace=True)
cases_total_yest = dfe[day_b_y].loc['Fälle total']
cases_total_today = df_final["Fälle total"].tail(1)
cases_new_today = df_final["Fälle neu"].tail(1)
nachmeldungen_cases = int(cases_total_today) - int(cases_total_yest) - int(cases_new_today)

#calculate Nachmeldungen Todesfälle
death_total_yest = dfe[day_b_y].loc['Todesfälle total']
death_total_today = df_final["Todesfälle total"].tail(1)
death_new_today = df_final["Todesfälle neu"].tail(1)
nachmeldungen_tod = int(death_total_today) - int(death_total_yest) - int(death_new_today)

#get Nachmeldungen from one week ago
date_in_df_prev += timedelta(days=1)
url_prev = url_yest.format(date_in_df_prev.date())
df_prev = pd.read_csv(url_prev)
df_prev.columns = ["a","b","c","d"]
df_prev.set_index('a', inplace=True)
nach_cases_prev = int(df_prev["b"].loc['Nachmeldungen Fälle'])
nach_tod_prev = int(df_prev["b"].loc['Nachmeldungen Todesfälle'])

#append to df
df_final["Nachmeldungen Fälle"] = [nach_cases_prev, nachmeldungen_cases]
df_final["Nachmeldungen Todesfälle"] = [nach_tod_prev, nachmeldungen_tod]


# In[14]:


#build first df without date (daily numbers)
df_final2 = df_final.loc[:, df_final.columns != "date"].T

#get date of latest numbers, change to string
date_current_values = df_final["date"].loc[1].date().strftime("%d.%m.%Y")
date_current_values = "Zahlen vom " + date_current_values

#rename columns
df_final2.columns = ["vor einer Woche", date_current_values]


# In[15]:


#build second df without date and calculate pct_change over one week (diff between daily numbers)
df_final3 = df_final.loc[:, df_final.columns != "date"].pct_change().multiply(100).round(1)
df_final3 = df_final3.T


# In[16]:


#concat daily numbers and difference
df_final4 = pd.concat([df_final2, df_final3], axis=1)

#drop not needed row and column
df_final4 = df_final4.drop(["index"])
df_final4 = df_final4.drop(columns=[0])


# In[17]:


#reorder columns
col_list = df_final4.columns.tolist()
myorder = [1,0,2]
col_list = [col_list[i] for i in myorder]
df_final4= df_final4[col_list]
df_final4.columns = [date_current_values, "vor einer Woche", "+/- in %"]

#fillna in diff column with 0
df_final4["+/- in %"] = df_final4["+/- in %"].fillna(0)

#replace nonsense-values with NaN
df_final4.loc[df_final4["+/- in %"] == np.inf, "+/- in %"] = np.nan
df_final4.loc[df_final4["+/- in %"] < -100, "+/- in %"] = np.nan
df_final4.loc[df_final4[date_current_values] < 0, "+/- in %"] = np.nan
df_final4.loc[df_final4["vor einer Woche"] < 0, "+/- in %"] = np.nan


# In[ ]:


#make a backup export of the current data
df_final4.to_csv("/root/covid_aargau/backups/daily_data/backup_{}.csv".format(backdate(0)))

#export to csv
df_final4.to_csv("/root/covid_aargau/data/only_AG/daily_data.csv")


# # daily new cases as line graph

# In[ ]:


df_dailys = df_cases.copy()
df_dailys2 = df_dailys[df_dailys["Neue Fälle"] > 0].copy()
df_dailys3 = df_dailys2[["date", "Neue Fälle", "7-Tages-Durchschnitt neue Fälle (+/- 3 Tage)"]].copy()
df_dailys3.columns = ["date", "Neue Fälle", "7-Tages-Durchschnitt"]
df_dailys3.reset_index(drop=True, inplace=True)


# In[ ]:


#add a baseline (for visualization purposes in Datawrapper)
df_dailys3["baseline"] = 0


# In[ ]:


#make a backup export of the current data
df_dailys3.to_csv("/root/covid_aargau/backups/daily_over_time/backup_{}.csv".format(backdate(0)))

#export to csv
df_dailys3.to_csv("/root/covid_aargau/data/only_AG/daily_over_time.csv", index=False)


# # hospital numbers

# In[ ]:


df_hosp = df[["date", "Bestätigte Fälle auf Abteilung (ohne IPS/IMC)", "Bestätigte Fälle IPS/IMC", "Restkapazität für Beatmung"]].copy()
df_hosp.set_index("date", inplace=True)
df_hosp.reset_index(inplace=True)


# In[ ]:


#if Monday (weekday == 0), take Friday as latest values
if date.today().weekday() == 0:
    df_hosp2 = df_hosp[df_hosp["date"] < backdate(2)]
else:
    df_hosp2 = df_hosp[df_hosp["date"] < backdate(0)]


# In[ ]:


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

