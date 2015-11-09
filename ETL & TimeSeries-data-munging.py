
# coding: utf-8
#import gc
#locals()
#globals()

import cx_Oracle
import pandas as pd
import numpy as np
import datetime
from blaze import dshape,Data
from odo import odo
import pyodbc
import pymssql
from sqlalchemy import create_engine, MetaData, Table, select
import traceback
import sys
import smtplib
import datetime
import time
import pdb
import os
import subprocess
dsnStr = cx_Oracle.makedsn("IP", "1521", "SCHEMA")
con = cx_Oracle.connect(user="username", password="password", dsn=dsnStr)



### A SERIES OF AGGREGATIONS AND QUERIES ON ORACLE DB
cur=con.cursor()
cur.execute("ORACLE  ...<sql>...")
con.commit()
cur.close()

cur=con.cursor()
cur.execute("ORACLE  ...<sql>...")
con.commit()
cur.close()

cur=con.cursor()
cur.execute("ORACLE  ...<sql>...")
con.commit()
cur.close()

cur=con.cursor()
cur.execute("ORACLE  ...<sql>...")
con.commit()
cur.close()

con.close()

### Read in User data and prepare for sorted tabular format and creating on new time delta variables
engine=create_engine('oracle+cx_oracle://username:password@ip:1521/schema::TABLE')
util2=pd.read_sql_table('utilization_table',engine,chunksize=10000000)
utils2=pd.concat(util2)
utils2['area']=utils2['area'].astype('category')
usr2=pd.read_sql_table('ups_and_chat_mg_wrk',engine,chunksize=20000000)
usrs2=pd.concat(usr2)
usrs2['area']=usrs2['area'].astype('category')
usrs2['classifier']=usrs2['classifier'].astype('category')



## Round up the minute timestamp to the nearest half-hour. This will allow this dataset to be merged with the other's 30 minute Time Intervals
ns30min=30*60*1000000000
usrs2['raw']=usrs2.min_stamp.astype(np.int64)
usrs2['min_increm']=pd.DatetimeIndex(usrs2['raw'].apply(lambda x: (((x //ns30min + 1) * ns30min))))
usrs2['min_increm']=pd.Series(usrs2['min_increm'])
usrs2['file_dt']=pd.to_datetime(usrs2['min_increm'])
del usrs2['raw'],usrs2['min_increm']


## Merge the Utilization Data with the now adjusted Timestamps in Sector-Min-Tail
util_ttl_usrs2=pd.merge(usrs2,utils2,how='left',on=['area','file_dt'])
util_ttl_usrs2['area']=util_ttl_usrs2['area'].astype('category')
util_ttl_usrs2['tail']=util_ttl_usrs2['tail'].astype('category')
util_ttl_usrs2['reference_number']=util_ttl_usrs2['reference_number'].astype('category')
#util_ttl_usrs2.info()
#util_ttl_usrs2.head()
print 'done merging 2 datasets'


## Remove all non-contributing (to chat) Minutes and calculate the 98th Pctl and Max Total User Value of series in dataframe. Drastically shrinking a dataset queried of length over 50mm
util_ttl_usrs2=util_ttl_usrs2[util_ttl_usrs2['classifier'] !=0]
util_ttl_usrs2=util_ttl_usrs2.sort(['area','min_stamp'])
util_ttl_usrs2['max_users']=util_ttl_usrs2.groupby(['reference_number'])['ttl_usrs'].transform(lambda x: x.max()).astype(np.int64)
util_ttl_usrs2['nineeighty_pctl_usrs']=util_ttl_usrs2.groupby(['reference_number'])['ttl_usrs'].transform(lambda x: x.quantile(.98).astype(np.int64))

util_ttl_usrs2.sort(['reference_number','min_stamp'])
df2=util_ttl_usrs2.drop_duplicates(subset=['reference_number'],take_last=True)

## USE PANDAS TO take much smaller and final table over to MySQL db for persistent storage

engine=create_engine("mysql+mysqldb://username:password@ip:3306/schema",pool_recycle=3600)
df2.to_sql(schema='schema',name='table',con=engine,if_exists='append', index=False,chunksize=100000)
