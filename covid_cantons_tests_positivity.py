#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime
import general_settings
from time import sleep


# # number of tests and positivity rate

# In[2]:


def csv_generator(canton):
    #read csv from Github
    source_url = "https://raw.githubusercontent.com/maekke/bag_data/master/daten_pro_kanton/bag_data_{}.csv".format(canton)
    source_url_all = "https://raw.githubusercontent.com/maekke/bag_data/master/bag_weekly_data.csv"
    
    df_testpos = pd.read_csv(source_url)
    sleep(2)
    df_testpos_all = pd.read_csv(source_url_all)
    
    df_testpos["positive_tests"] = (df_testpos["total_number_of_tests"] / 100 * df_testpos["positivity_rate_percent"]).round(0).astype(int)
    df_testpos["negative_tests"] = df_testpos["total_number_of_tests"] - df_testpos["positive_tests"]
    
    #convert calendar week to first day of respective week (date)
    def get_weekday(s):
        prefix = "2020-W"
        cal_week = prefix + str(s)
        first_weekday = datetime.strptime(cal_week + '-1', "%G-W%V-%u")
        return first_weekday

    df_testpos["week_date"] = df_testpos["week"].apply(get_weekday)
    df_testpos_all["week_date"] = df_testpos_all["week"].apply(get_weekday)

    #formatting
    df_testpos["week_date"] = df_testpos["week_date"].dt.strftime("%d.%m.%Y")
    df_testpos_all["week_date"] = df_testpos_all["week_date"].dt.strftime("%d.%m.%Y")
    df_testpos.columns = ["Kalenderwoche",
                             "Tests total",
                             "Positivitätsrate",
                            "source_file",
                            "positiv",
                            "negativ",
                            "Datum"]
    
    #get relevant columns
    df_tests = df_testpos[["Datum", "positiv", "negativ"]].T
    df_pos = df_testpos[["Kalenderwoche", "Datum", "Positivitätsrate"]]
    df_pos = df_pos.join(df_testpos_all["positivity_rate_percent"])
    df_pos.columns = ["Kalenderwoche", "Datum", canton, "Schweiz"]

    #make a backup export of the current data
    df_tests.to_csv("/root/covid_aargau/backups/tests/tests_weekly_{}_{}.csv".format(canton, general_settings.today))
    df_pos.to_csv("/root/covid_aargau/backups/positivity/positivity_weekly_{}_{}.csv".format(canton, general_settings.today))


    #export to csv
    df_tests.to_csv("/root/covid_aargau/data/tests_weekly_{}.csv".format(canton), header=None)
    df_pos.to_csv("/root/covid_aargau/data/positivity_weekly_{}.csv".format(canton), index=False)


# In[3]:


cantons = ["AG", "SG", "LU"]

for canton in cantons:
    csv_generator(canton)
    sleep(5)

