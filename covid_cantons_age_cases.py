#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from time import sleep
from general_settings import backdate


# In[2]:


def age_dist_cantons(canton):
    base_url = "https://raw.githubusercontent.com/timoll/bag_scrape/master/out/canton_age/"
    url_men = base_url + canton + "/" + "men.csv"
    url_women = base_url + canton + "/" + "women.csv"
    url_total = base_url + canton + "/" + "all.csv"
    
    df_men = pd.read_csv(url_men)
    sleep(5)
    df_women = pd.read_csv(url_women)
    sleep(5)
    df_total = pd.read_csv(url_total)
    
    s_men_sum = df_men.loc[:, df_men.columns != "date"].sum()
    s_women_sum = df_women.loc[:, df_women.columns != "date"].sum()
    s_total_sum = df_total.loc[:, df_total.columns != "date"].sum()

    df_men_sum = pd.DataFrame(s_men_sum)
    df_men_sum = df_men_sum.reset_index()
    df_men_sum = df_men_sum.rename(columns={"index": "Altersklasse", 0: "Männer"})

    df_women_sum = pd.DataFrame(s_women_sum)
    df_women_sum = df_women_sum.reset_index()
    df_women_sum = df_women_sum.rename(columns={"index": "Altersklasse", 0: "Frauen"})

    df_total_sum = pd.DataFrame(s_total_sum)
    df_total_sum = df_total_sum.reset_index()
    df_total_sum = df_total_sum.rename(columns={"index": "Altersklasse", 0: "Total"})
    
    df_gen = df_men_sum.merge(df_women_sum, on="Altersklasse")
    df_all = df_gen.merge(df_total_sum, on="Altersklasse")
    df_all.drop(df_all.tail(1).index,inplace=True)
    
    #export backup to csv
    df_all.to_csv("/root/covid_aargau/backups/age/altersverteilung_{}_{}.csv".format(canton, backdate(0)), index=False)
    
    #export to csv
    df_all.to_csv("/root/covid_aargau/data/altersverteilung_{}.csv".format(canton), index=False)


# In[3]:


cantons = ["AG", "SG", "LU", "TG"]

for canton in cantons:
    age_dist_cantons(canton)
    sleep(5)

