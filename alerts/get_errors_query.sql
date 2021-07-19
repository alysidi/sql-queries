WITH state_changes AS 
      ( 
                 SELECT     window_of_state_changes.*
                 FROM (VALUES ('0001000823F0','00010007110F'),('000100083518', '000100072FA2') ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 --FROM (VALUES {} ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 CROSS JOIN lateral 
                            ( 
                                     SELECT   device_id, 
                                              host_rcpn, 
                                              timestamp_utc, 
                                              st
                                     FROM     status.legacy_status_state_change lssc 
                                     WHERE    device_id = parent.device_id 
                                     AND      timestamp_utc >= NOW() - INTERVAL '12 hours'
                    
                             ) as window_of_state_changes    
        ),

        bad_states AS (
                        select device_id, to_hex(st) as state, count(st) as state_count, min(timestamp_utc) as transition_timestamp_utc
                        from state_changes
                        where st BETWEEN x'7000'::int AND x'7FFF'::int 
                        group by device_id, st
          )
  

      SELECT * FROM  (
        SELECT   device_id,
                 host_rcpn, 
                 'DEVICE_IN_ERROR_STATE' as alert_type,
                 timestamp_utc as last_heard_timestamp_utc,
                 to_hex(st) as latest_state
          
        FROM     status.device_shadow 
        WHERE device_id in (select distinct device_id from bad_states)
        GROUP BY device_id, host_rcpn, latest_state, last_heard_timestamp_utc
      ) parent
      
      CROSS JOIN LATERAL (

        select state, transition_timestamp_utc
        from bad_states 
        where device_id = parent.device_id
                  ) as transition_timestamp

       CROSS JOIN LATERAL (

                      select json_build_object('total', count(t.*), 'transitions', json_agg(to_json(t))) as num_transitions_in_window
                      from (
                       select state, state_count
                        from bad_states
                        where device_id = parent.device_id
                      ) t
                  ) as bad_state_counts

       CROSS JOIN LATERAL (

                      select json_build_object( 'history', json_agg(to_json(t))) as history
                      from (
                        select st as state,
                        lag(st) OVER (partition BY device_id ORDER BY timestamp_utc DESC) AS next_state,
                        timestamp_utc,
                        lag(timestamp_utc) OVER (partition BY device_id ORDER BY timestamp_utc DESC) AS next_state_timestamp
                        from status.legacy_status_state_change
                        where device_id = parent.device_id
                        and timestamp_utc > now() - INTERVAL '30 minutes'
                        order by timestamp_utc desc
                        LIMIT 100
                      ) t
                  ) as history_of_state_changes
                