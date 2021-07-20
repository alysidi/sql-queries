import pandas as pd
from psycopg2 import pool
import math
import numpy as np
import os
import time
start_time = time.time()


# db settings from env
username = os.environ.get('TS_STG_USERNAME')
password = os.environ.get('TS_STG_PASSWORD')


# sql queries
with open('get_errors_query.sql', 'r') as file:
  get_errors_query = file.read()


def index_marks(nrows, chunk_size):
    return range(chunk_size, math.ceil(nrows / chunk_size) * chunk_size, chunk_size)

def split(dfm, chunk_size):
    indices = index_marks(dfm.shape[0], chunk_size)
    return np.split(dfm, indices)



# use connection pool for single threaded apps. replace credentials with Timescale user pool settings
postgres_pool = pool.SimpleConnectionPool(1, 5, user=username,
                                                         password=password,
                                                         host="timescaledb-stg.neur.io",
                                                         port="12949",
                                                         database="defaultdb")


with postgres_pool.getconn() as conn:

    conn.set_session(autocommit=True) # need this line to avoid idle_in_transaction in Postgres
    
    # get all devices from shadow that are active in the last month. We can remove this filter if needed.
    sql = "select device_id, host_rcpn from status.device_shadow {where_clause} order by timestamp_utc desc {limit};".format(where_clause="where timestamp_utc > now() - interval '1 month' and device_type='PVLINK'",limit="")
    df = pd.read_sql_query(sql, conn)

    # chunk settings
    total_device_shadow_devices = len(df);
    chunk_size = int(total_device_shadow_devices * 0.025) # between 2.5% and 5% should be good for batching performance
    print("chunk size:",chunk_size, "total devices:", total_device_shadow_devices)

    # split dataframe into chunk for bulk inserts
    chunks = split(df, chunk_size)
    for chunk in chunks:
      # convert dataframe chunk to tuples
      records = chunk.to_records(index=False)
      tuples = str(list(records))[1:-1]


      # template sql query with chunked tuples
      query = get_errors_query.format(tuples)
      # print(query)

      # get output error batch -> send on for further processing
      df2 = pd.read_sql_query(query, conn)
      # print(df2)
      # print(df2.to_json(orient='records'))

      print("--- %s seconds ---" % (time.time() - start_time))

      