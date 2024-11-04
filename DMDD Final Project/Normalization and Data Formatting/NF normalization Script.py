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
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# In[7]:


barens = pd.read_csv('barnes.csv')


# engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# # conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# # cursor = conn.cursor()

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor("create table barens(prod_name varchar(30),category varchar(30),Price decimal(5,5), webiste varchar(30))")



# df.to_sql(name = 'barnes', con = engine, if_exists='append', index = False)


df = pd.DataFrame(barens, columns=['prod_name', 'category','Price','webiste'])


# In[105]:


ba = barens.drop (['prod_name'], axis = 1)


# In[43]:


df['prod_volume'] = df['prod_name'].str.split(',')


# In[ ]:


df


# In[69]:


df_models = pd.DataFrame(df['prod_volume'].tolist()).fillna('').add_prefix('column')


# In[99]:


df_models.drop(['column2'], axis=1)


# In[106]:


dffinal = pd.concat([ba, df_models], axis=1)


# In[107]:


dffinal


# In[110]:


dffinal.to_sql(name = 'barenfinal', con = engine, if_exists='append', index = False)


# In[93]:


dffinal1.to_sql(name = 'barnesfinal', con = engine, if_exists='append', index = False)


# In[82]:


df_final = df['prod_name'],df['Price']


# In[83]:


df_final 


# In[71]:


df['volume_column1']


# In[62]:


barens = pd.read_csv('barnes.csv')


engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor()

conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
cursor = conn.cursor("create table barens(prod_name varchar(30),category varchar(30),Price decimal(5,5), webiste varchar(30))")



df.to_sql(name = 'barnes', con = engine, if_exists='append', index = False)


df = pd.DataFrame(barens, columns=['prod_name', 'category','Price','webiste'])


# In[ ]:





# In[58]:


a = np
m1 = np.array([[1,12],[2,12],[3,12],[4,12],[5,12],[6,12],[7,12],[8,12],[9,12],[10,12],[11,12],[12,12]])
m2 = np.array([[1,2],[1,2]])
print(np.matmul(m1,m2))


# In[111]:


amazon_df = pd.read_csv('marketing_sample_for_amazon_com-ecommerce__20200101_20200131__10k_data (1).csv')
# amazon_df.head()
# engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor()


# cursor.execute("create table amazon(Uniq_Id varchar(70),Product_Name varchar(200),Brand_Name varchar(200),Category varchar(200),Selling_Price varchar(500),Model_Number varchar(100),Product_Specification varchar(1000),Shipping_Weight varchar(300),Product_Url varchar(1000),Is_Amazon_Seller varchar(250))")
# amazon_df.to_sql('amazon', con = engine, if_exists='append', index = False)


# In[113]:


amazon_df


# In[119]:


amzon_2NF= amazon_df.drop(['Uniq_Id','Brand_Name','Model_Number','Shipping_Weight','Selling_Price'], axis=1)


# In[124]:


amazon_p = amzon_2NF.drop(['Is_Amazon_Seller'],axis = 1)


# In[125]:


amazon_p.to_sql(name = 'amazon_product', con = engine, if_exists='append', index = False)


# In[128]:


import datetime;
walmart_df = pd.read_csv('Walmartcsvgheabhishek.csv')

# engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')
# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# # conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# # cursor = conn.cursor()

# mycursor = conn.cursor()
# current_time = datetime.datetime.now()

# cursor = conn.cursor("CREATE TABLE walmartdata(Uniq_Id varchar(150),Crawl_Timestamp varchar(200),Product_Url varchar(1000),Product_Name varchar(500),Sale_Price DECIMAL(10,5),Brand varchar(500),Item_Number int,Gtin varchar(100),Product_inovation varchar(30),Category varchar(500), Available varchar(50),webiste varchar(30))")
# #  x = conn.cursor("select * from trig")
# # sql =  conn.cursor("insert into trig(%s,%s)")

# # val = [("walmart",current_time) ]
# # mycursor.execute(sql,val)
# # # print(current_time)

# walmart_df.to_sql(name = 'walmartdata', con = engine, if_exists='append', index = False)


# In[129]:


walmart_df


# In[136]:


walmart_p =  walmart_df.drop(['Uniq_Id','Crawl_Timestamp','Sale_Price','Item_Number','Gtin','webiste'],axis = 1)


# In[138]:


walmart_p 


# In[139]:


walmart_prod = walmart_p.drop(['Available'],axis =1)


# In[140]:


walmart_prod.to_sql(name = 'walmart_product', con = engine, if_exists='append', index = False)


# In[141]:


walmart_prod


# In[142]:


ebay = pd.read_csv('ebay.csv')

# engine = create_engine('mysql://root:ABHIpatil123#@localhost:3306/betterpurchase')

# # conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# # cursor = conn.cursor()

# conn = msql.connect(host='localhost', database='betterpurchase', user='root', password='ABHIpatil123#')
# cursor = conn.cursor("create table ebay(prod_name varchar(200),category varchar(200),price decimal(5,5),rating int,webiste varchar(30))")



# df.to_sql(name = 'ebay', con = engine, if_exists='append', index = False)


# In[144]:


ebay


# In[145]:


apple_df = pd.read_csv('apple exchange rate.csv')


# In[146]:


apple_df


# In[147]:


apple_pro_df = pd.read_csv('apple product price list from 26 countries.csv')


# In[148]:


apple_pro_df


# In[150]:


apple_pro =  apple_pro_df.drop(['pid','country_code','scraped_date'],axis = 1)


# In[151]:


apple_pro


# In[153]:


apple_pro1 =  apple_pro.drop(['incomegroup','region'],axis = 1)


# In[156]:


apple_pro1.to_sql(name = 'appleproducts', con = engine, if_exists='append', index = False)


# In[157]:


income_group = apple_pro.drop(['model','price'],axis = 1)


# In[158]:


income_group.to_sql(name = 'appleproductincomegroup', con = engine, if_exists='append', index = False)


# In[ ]:




