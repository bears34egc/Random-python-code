import json
import urllib
import requests
import urllib2
from nltk.misc import babelfish
import unicodedata
import pandas.io.sql
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, String
import sys
import datetime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

## CALL A MSSQL STORED PROCEDURE FROM PRODUCTION DB IN MSSQL. THIS .sp WILL AGGREGATE TO DAILY LEVEL, METRICS
try:
    server = "SQLEXPRESS"
    db = "TABLEAU"

    engine=create_engine("mssql+pyodbc://ODBCDriver/")
    Base.metadata.create_all(engine)
    connection=engine.connect()
    trans=connection.begin()
    try:
    	r1=connection.execute("[dbo].[CSAT_NSAT_pull]")
    	trans.commit()
    except:
    	trans.rollback()
        print 'ISSUE!'
    	raise
    connection.close()
    print 'stored procedure complete'
    
## MOVE THE DATA USING PANDAS TO MYSQL DB WHERE SPACE IS AVAIL, REFORMAT
    df=pd.read_sql_table("CHATS",engine,index_col=None,coerce_float=True,parse_dates=['CHAT_REQUEST_DATE'])
    print 'data read'
    print df.info()
    print len(df)
    df = df.where((pd.notnull(df)), None)
    engine.dispose()


    engine=create_engine("mysql+mysqldb://username:password@localhost:3306/schema")
    df.to_sql(schema='schema',name='chats', con=engine,if_exists='replace', index=False,chunksize=100000)
    print 'incr data transferred to mysql'

    print 'complete'
## IN CASE OF ANY ERRORS RECEIVE EMAIL
except Exception,err:
    import traceback
    import sys
    import smtplib
    import datetime
    import time
    e=str(traceback.format_exc())
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    fromMy = 'name@yahoo.com'
    to = 'name@outlook.com'
    subj='mysql_pandas.py'
    date=str(st)
    message_text=date + e
    msg = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( fromMy, to, subj, date, message_text )

    username=str('username')
    password=str('password')

    server=smtplib.SMTP("smtp.mail.yahoo.com",587)
    server.starttls()
    server.login(username,password)
    server.sendmail(fromMy,to,msg)
    server.quit()
    print 'ok the email has sent'
