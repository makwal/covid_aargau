#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from general_settings import datawrapper_api_key
from time import sleep


# In[2]:


#url BAG
base_url = 'https://www.covid19.admin.ch/api/data/context'

#url + credentials Datawrapper
datawrapper_url = 'https://api.datawrapper.de/v3/charts/'
headers = {'Authorization': datawrapper_api_key}


# In[3]:


chart_ids = {
    'AG daily': 'dAo3T',
    'AG cases short': '1zRdb',
    'AG cases long': 'OqLEv',
    'AG hosp short': 'zgWTi',
    'AG hosp long': '7kR7N',
    'AG infection venues': 'Xzk0f',
    'SO daily': 'vp5VA',
    'SO cases': 'LX6pQ',
    'BS daily': 'oXWtD',
    'BL daily': 'puWef'
}


# In[4]:


def chart_updater(chart_id):

    url_publish = datawrapper_url + chart_id + '/publish'
    
    res_publish = requests.post(url_publish, headers=headers)


# In[5]:


for chart, chart_id in chart_ids.items():
    chart_updater(chart_id)
    sleep(3)

