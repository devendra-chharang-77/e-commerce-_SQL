#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


pip install pandas


# In[2]:


pip install mysql-connector-python 


# In[12]:


import pandas as pd
import mysql.connector
import os

csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),        
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'), 
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')
]

conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='devendra123',
    database='ecommerce',
    use_pure=True
)
cursor = conn.cursor()
print("✅ Database Connected Successfully!")

folder_path = r'C:\Users\LENOVO\Downloads\ecommerce'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path)
    df = df.where(pd.notnull(df), None)
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    
    print(f"Processing {csv_file} ({len(df)} rows)")
    
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Chunk size - 5000 rows at a time
    chunk_size = 5000
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        data_to_insert = [tuple(x) for x in chunk.to_numpy()]
        placeholders = ', '.join(['%s'] * len(df.columns))
        cols = ', '.join(['`' + col + '`' for col in df.columns])
        sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
        cursor.executemany(sql, data_to_insert)
        conn.commit()
        print(f"   Inserted rows {i} to {i+len(chunk)}")

    print(f"'{table_name}' done!\n")

conn.close()
print(" All tables created successfully!")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




