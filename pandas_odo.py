import pandas as pd
import numpy as np
import datetime
from blaze import dshape,Data
from odo import odo
from sqlalchemy import create_engine, MetaData, Table, select


dir='C:\\Documents\\'
# names=['concur_tails','site_id','sector','site_sector','min_stamp','day_stamp','tail','technology','aircards','departure_airport','arrival_airport','departure_time','departure_day','arrival_time','arrival_day','flight_number']
# df=pd.read_csv(dir + 'atg_perf.csv',names=names,skiprows=1,parse_dates=True,dtype={'tail':np.str,'technology':np.str,'aircards':np.str,'departure_airport':np.str,'arrival_airport':np.str,'flight_number':np.str},engine='c')
ds=dshape("var * {concur_tails:float64,site_id:float64,sector:float64,site_sector:string,min_stamp:datetime,day_stamp:datetime,tail:string,technology:string,aircards:string,departure_airport:string,arrival_airport:string,departure_time:datetime,departure_day:datetime,arrival_time:datetime,arrival_day:datetime,flight_number:string}")
d=Data(dir + 'perf.csv',dshape=ds)
print d.dshape
df=odo(d,pd.DataFrame)

print df.info()
df = df.where((pd.notnull(df)), None)
df.departure_time=pd.to_datetime(df.departure_time)
df.arrival_time=pd.to_datetime(df.arrival_time)
df.min_stamp=pd.to_datetime(df.min_stamp)

df['takeoff_incr']=(df.min_stamp-df.departure_time).astype('timedelta64[m]')
df['landing_incr']=(df.arrival_time-df.min_stamp).astype('timedelta64[m]')
print 'timestamps complete'

df['unique_flt_id']=df['flight_number'].map(str) + ' ' + df['departure_time'].apply(lambda x: x.strftime('%d%m%Y %H%M%S'))

df['min_delta']=1


grouped=df.groupby(['site_sector'])
df['min_per_site']=(grouped['min_delta'].transform(lambda x: x.sum())).astype(np.int64)

grouped=df.groupby(['departure_airport','arrival_airport'])
df['flight_ct']=(grouped['unique_flt_id'].transform(lambda x: x.nunique())).astype(np.int64)
df['min_p_site__citypair_flight']=(df.min_per_site/df.flight_ct).astype(np.int64)
print df.info()
df.to_csv('perf_cln.csv',index=False)

engine=create_engine("mysql+mysqldb://username:password@localhost:3306/schema",pool_recycle=3600)
df.to_sql(schema='schema',name='routes',if_exists='replace',con=engine,index=False)
