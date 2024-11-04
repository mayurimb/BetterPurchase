#!/usr/bin/env python
# coding: utf-8

# In[2]:


import configparser
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector as msql
from mysql.connector import Error
import csv,sys
import sqlite3
import re


# In[3]:


import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# In[207]:


x= np.linspace(0,5,100)
y= np.sin(4*x)*np.exp(-x)
z= np.cos(4*x)*np.exp(-x)
plt.figure()
plt.plot(x,z)


# In[ ]:





# In[202]:


consistancy = pd.read_csv( 'walmart.csv')


# In[ ]:





# In[210]:


plt.figure()
con = consistancy.loc[1:20, ['Sale_Price', 'Crawl_Timestamp']]
plt.plot(con.Crawl_Timestamp,con.Sale_Price)


# In[217]:


con = consistancy.loc[1:20, ['Gtin', 'Sale_Price']]
plt.plot(con.Gtin,con.Sale_Price)


# In[ ]:


### comparing data #########


# In[254]:


test = pd.read_csv('apple product price list from 26 countries.csv')
# test1 = test.query("country = 'United States'")
us = test[test.country == 'United States']
india = test[test.country == 'India']
# india = (test['country'] == 'India')


# In[255]:


print(us)


# In[ ]:


plt.plot(con.Gtin,con.Sale_Price)


# In[248]:


num = re.findall(r'\d+', str(us.price)) 


# In[256]:


num2 = re.findall(r'\d+', str(india.price)) 


# In[272]:


x= [num[0],num[1],num[2],num[3],num[4],num[5],num[6],num[7],num[8],num[9]]
y = [num2[0],num2[1],num2[2],num2[3],num2[4],num2[5],num2[6],num2[7],num2[8],num2[9]]


# In[270]:


print(x)


# In[ ]:





# In[249]:


print(num)


# In[257]:


print(num2)


# In[273]:


plt.plot(x,y)


# In[ ]:





# In[ ]:


#### amazon #####


# In[30]:


amazon_df = pd.read_csv('marketing_sample_for_amazon_com-ecommerce__20200101_20200131__10k_data.csv')
amazon_df.head()
engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor()


cursor.execute("create table amazon(Uniq_Id varchar(70),Product_Name varchar(200),Brand_Name varchar(200),Category varchar(200),Selling_Price varchar(500),Model_Number varchar(100),Product_Specification varchar(1000),Shipping_Weight varchar(300),Product_Url varchar(200),Is_Amazon_Seller varchar(250))")
amazon_df.to_sql('amazon', con = engine, if_exists='append', index = False)


# In[77]:


amazon_df = pd.read_csv('marketing_sample_for_amazon_com-ecommerce__20200101_20200131__10k_data (1).csv')
amazon_df.head()
engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor()


cursor.execute("create table amazon(Uniq_Id varchar(70),Product_Name varchar(200),Brand_Name varchar(200),Category varchar(200),Selling_Price varchar(500),Model_Number varchar(100),Product_Specification varchar(1000),Shipping_Weight varchar(300),Product_Url varchar(1000),Is_Amazon_Seller varchar(250))")
amazon_df.to_sql('amazon', con = engine, if_exists='append', index = False)


# In[6]:


+


# In[172]:


df.describe()


# In[ ]:





# In[ ]:


############ Apple official website ###########


# In[100]:


df = pd.read_csv('apple product price list from 26 countries.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')
 
conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("create table apple_product_price(pid varchar(50),model varchar(80), price varchar(80), country_code varchar(50), country varchar(80),region varchar(80),incomegroup varchar(80),date varchar(80))")


cursor.execute("")
df.to_sql('apple_product_price', con = engine, if_exists='append', index = False)


# In[ ]:


##### apple currency code#######


# In[32]:


df = pd.read_csv('apple exchange rate.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("create table apple_exchange_rate(Date varchar(30),CurrencyCodeA varchar(40),X DECIMAL(8,5))")

cursor.execute("")
df.to_sql('apple_exchange_rate', con = engine, if_exists='append', index = False)


# In[33]:


df.describe()


# In[ ]:


#### bestbuy ############


# In[95]:


df = pd.read_csv('DatafinitiElectronicsProductData.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("")

df.to_sql('bestbuy', con = engine, if_exists='append', index = False)


# In[ ]:


#### walmart #######


# In[96]:


import datetime;
walmart_df = pd.read_csv('Walmartcsvgheabhishek.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')
conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor()

mycursor = conn.cursor()
current_time = datetime.datetime.now()

cursor = conn.cursor("CREATE TABLE walmartdata(Uniq_Id varchar(150),Crawl_Timestamp varchar(200),Product_Url varchar(1000),Product_Name varchar(500),Sale_Price DECIMAL(10,5),Brand varchar(500),Item_Number int,Gtin varchar(100),Product_inovation varchar(30),Category varchar(500), Available varchar(50),webiste varchar(30))")
#  x = conn.cursor("select * from trig")
# sql =  conn.cursor("insert into trig(%s,%s)")

# val = [("walmart",current_time) ]
# mycursor.execute(sql,val)
# # print(current_time)

walmart_df.to_sql(name = 'walmartdata', con = engine, if_exists='append', index = False)


# In[56]:


walmart_df.count();


# In[ ]:


audit data validation


# In[ ]:





# In[10]:


amazon_df.isnull().sum()


# In[ ]:





# In[74]:


walmart_df.nunique()


# In[12]:


amazon_df.count()


# In[90]:


df = pd.read_csv('ebay.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor()

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("create table ebay(prod_name varchar(200),category varchar(200),price decimal(5,5),rating int,webiste varchar(30))")



df.to_sql(name = 'ebay', con = engine, if_exists='append', index = False)


# In[91]:


df = pd.read_csv('barnes.csv')

engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor()

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("create table barens(prod_name varchar(30),category varchar(30),Price decimal(5,5), webiste varchar(30))")



df.to_sql(name = 'barnes', con = engine, if_exists='append', index = False)


# In[2]:


ebay = pd.read_csv('ebay.csv')

# engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# # conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# # cursor = conn.cursor()

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor("create table ebay(prod_name varchar(200),category varchar(200),price decimal(5,5),rating int,webiste varchar(30))")



# df.to_sql(name = 'ebay', con = engine, if_exists='append', index = False)


# In[ ]:




