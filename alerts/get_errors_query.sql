
WITH state_changes AS 
      ( 
                 SELECT     window_of_state_changes.*
                 --FROM (VALUES ('0001000823F0','00010007110F'),('000100083518', '000100072FA2') ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 FROM (VALUES {} ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks                 
                 JOIN  
                            ( 
                                     SELECT   device_id, 
                                              host_rcpn, 
                                              extract(epoch from min(timestamp_utc) ) as transition_timestamp_utc,                                              device_type, 
                                              to_hex(st) as device_state, 
                                              count(st) as count
                                     FROM     status.legacy_status_state_change 
                                     WHERE    st between x'7000'::int and x'7FFF'::int
                                     AND      timestamp_utc >= NOW() - INTERVAL '12 hours'
                                     GROUP BY device_id, 
                                              host_rcpn, 
                                              device_type, 
                                              st
                    
                             ) as window_of_state_changes   
                  ON    window_of_state_changes.device_id = parent.device_id 
        )


      SELECT * FROM  (
        SELECT   device_id,
                 host_rcpn, 
                 device_type,
                 'DEVICE_IN_ERROR_STATE' as alert_category,
                 extract(epoch from timestamp_utc) as latest_timestamp_utc,
                 to_hex(st) as latest_device_state
          
        FROM     status.device_shadow 
        WHERE device_id in (select distinct device_id from state_changes)
        GROUP BY device_id, host_rcpn, device_type, latest_device_state, latest_timestamp_utc
      ) parent
      
    
       CROSS JOIN LATERAL (

                      select json_agg(to_json(t)) as num_of_errors_in_window
                      from (
                       select device_state as error_device_state, count
                        from state_changes
                        where device_id = parent.device_id
                      ) t
                  ) as bad_state_counts

       CROSS JOIN LATERAL (

                      select json_agg(to_json(t)) as device_state_transition_history
                      from (
                        select to_hex(st) as current_device_state,
                          CASE
                              WHEN lag(to_hex(st)) OVER (partition BY device_id
                                                         ORDER BY timestamp_utc DESC) IS NULL THEN 'NULL'
                              ELSE lag(to_hex(st)) OVER (partition BY device_id
                                                         ORDER BY timestamp_utc DESC)
                          END 
                          as next_device_state,
                        extract(epoch from timestamp_utc) as current_timestamp_utc,
                        
                        CASE
                          WHEN lag(extract(epoch
                                           FROM timestamp_utc)) OVER (partition BY device_id
                                                                      ORDER BY timestamp_utc DESC) IS NULL THEN 0
                          ELSE lag(extract(epoch
                                           FROM timestamp_utc)) OVER (partition BY device_id
                                                                      ORDER BY timestamp_utc DESC)
                        END
                         as next_timestamp_utc
                        from status.legacy_status_state_change
                        where device_id = parent.device_id
                        order by timestamp_utc desc
                        LIMIT 50
                      ) t
                  ) as history_of_state_changes
                
