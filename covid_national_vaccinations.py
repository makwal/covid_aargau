#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from general_settings import backdate


# In[ ]:


#url vaccination data
base_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"

#url vaccine types
add_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/locations.csv"

#url german country names
countries_url = "https://raw.githubusercontent.com/stefangabos/world_countries/master/data/de/countries.csv"


# In[ ]:


df_import = pd.read_csv(base_url)
df_import_add = pd.read_csv(add_url)
df_import_countries = pd.read_csv(countries_url)

df_import_add = df_import_add[["iso_code", "vaccines"]]

#merge primary dataframe with vaccine type data
df_import_m = df_import.merge(df_import_add, left_on="iso_code", right_on="iso_code")

df = df_import_m[["iso_code", "location", "total_vaccinations_per_hundred", "date", "vaccines"]].copy()

#get latest value for each country with groupby, sort df
df2 = df.groupby("location").tail(1).sort_values(by="total_vaccinations_per_hundred", ascending=False)

#prepare german country names and iso_codes
dfc = df_import_countries[["name", "alpha2", "alpha3"]].copy()
dfc["alpha3"] = dfc["alpha3"].str.upper()

#merge primary dataset with german country names
df3 = df2.merge(dfc, left_on="iso_code", right_on="alpha3")

#formatting for datawrapper visualization purposes
df3["date"] = pd.to_datetime(df3["date"]).dt.strftime("%d.%m.%Y")
df3["name"] = df3["name"].astype(str) + " ^" + df3["date"] + "^"
df3["alpha2"] = ":" + df3["alpha2"] + ": " + df3["name"]

#create df_final
df_final = df3[["alpha2", "total_vaccinations_per_hundred", "vaccines"]].copy()
df_final.columns = ["Land ^aktualisiert^", "Impfungen pro 100 Menschen", "Impfstoffe"]


# In[ ]:


#make a backup export of the current data
df_final.to_csv("/root/covid_aargau/backups/vacc_world/backup_{}.csv".format(backdate(0)), index=False)

#export to csv
df_final.to_csv("/root/covid_aargau/data/vaccination/vacc_world.csv", index=False)

