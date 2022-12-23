#!/usr/bin/env python
# coding: utf-8

# # Dataset 1 - person 

# In[1]:


# Importing Libraries
import pandas as pd
import numpy as np
import requests,json


# In[2]:


# Getting Data from API 
try:
    URL = 'https://data.cityofnewyork.us/resource/f55k-p6yu.json'
    df = pd.read_json(URL)
except:
    print("Failed to get json data")


# In[3]:


# Listing Columns of Dataframe
print(list(df.columns))


# In[4]:


print(df.head())


# In[5]:


# Diaplay of Dataframe
display(df)


# In[6]:


# Connection To MongoDB
import pymongo


# In[7]:


myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb = myclient["Database_1"]


# In[8]:


mycol = mydb["Collection_1"]


# In[9]:


df = df.applymap(str)


# In[11]:


#Inserting dataframe in collection

mycol.insert_many(df.to_dict(orient='records'))


# In[18]:


# Appending mongoDB series object to Dataframe Object

records = mycol.find()

col = mycol
print ("total docs in collection:", col.count_documents( {} ))
mongo_docs = list(records)

docs1 = pd.DataFrame(columns=list(df.columns))

for num, doc in enumerate( mongo_docs ):
    doc["_id"] = str(doc["_id"])
    doc_id = doc["_id"]
    series_obj = pd.Series(doc, name=doc_id)
    docs1 = docs1.append(series_obj)


# In[17]:


display(type(docs1))

