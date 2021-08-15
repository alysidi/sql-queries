import pandas as pd
from psycopg2 import pool
import math
import numpy as np
import os
import time
start_time = time.time()



# use connection pool for single threaded apps. replace credentials with Timescale user pool settings
postgres_pool = pool.SimpleConnectionPool(1, 5, user='root',
                                                         password='password',
                                                         host="localhost",
                                                         port="5432",
                                                         database="defaultdb")


with postgres_pool.getconn() as conn:

    conn.set_session(autocommit=True) # need this line to avoid idle_in_transaction in Postgres
    cursor = conn.cursor()
    # get all devices from shadow that are active in the last month. We can remove this filter if needed.
    sql_regular = """
      INSERT INTO status.device_shadow
      SELECT NOW() as timestamp_utc, CONCAT('device',n) as device_id, CONCAT('beacon',n) as beacon_rcpn,  CONCAT('host_rcpn',n) as host_rcpn, 'INVERTER' as device_type, 10 as st 
      FROM generate_series(1,100000,1) as n
      ON CONFLICT (device_id,host_rcpn) DO UPDATE
      SET timestamp_utc = now()+INTERVAL '10 minutes';
    """

    sql_hyper = """
    BEGIN;
    DELETE FROM status.device_shadow_hyper;
    INSERT INTO status.device_shadow_hyper
    SELECT NOW() + INTERVAL '10 minute' as timestamp_utc, CONCAT('device',n) as device_id, CONCAT('beacon',n) as beacon_rcpn,  CONCAT('host_rcpn',n) as host_rcpn, 'INVERTER' as device_type, 10 as st 
    FROM generate_series(1,100000,1) as n;
    COMMIT;
    """

    while(1==1):
      cursor.execute(sql_hyper)
      conn.commit() # <- We MUST commit to reflect the inserted data
      time.sleep(1)


    #select * from status.device_shadow where device_id='device1'
    

    

  