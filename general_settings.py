#!/usr/bin/env python
# coding: utf-8

# In[14]:


from datetime import datetime, date, timedelta


# In[15]:


file_url = "https://www.ag.ch/media/kanton_aargau/themen_1/coronavirus_1/daten_excel/Covid-19-Daten_Kanton_Aargau.xlsx"


# In[16]:


today = date.today()
today = today.strftime("%Y-%m-%d")

todays_weekday = date.today().weekday()

yesterday = date.today() - timedelta(days=1)
yesterday = yesterday.strftime("%Y-%m-%d")

two_days_ago = date.today() - timedelta(days=2)
two_days_ago = two_days_ago.strftime("%Y-%m-%d")

three_days_ago = date.today() - timedelta(days=3)
three_days_ago = three_days_ago.strftime("%Y-%m-%d")

four_days_ago = date.today() - timedelta(days=3)
four_days_ago = four_days_ago.strftime("%Y-%m-%d")


# In[20]:


def backdate(n):
    return (date.today() - timedelta(days=n)).strftime("%Y-%m-%d")

