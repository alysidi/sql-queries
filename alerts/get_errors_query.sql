WITH state_changes AS 
      ( 
                 SELECT     p.* 
                 FROM       (VALUES {} ) AS parent(device_id, host_rcpn) -- pass in tuples from chunks
                 CROSS JOIN lateral 
                            ( 
                                     SELECT   device_id, 
                                              host_rcpn, 
                                              timestamp_utc, 
                                              st AS device_state,
                                              lag(st) OVER (partition BY device_id ORDER BY timestamp_utc DESC) AS next_state,
                                              -- delta in hours between states. If no next state, use current time 
                                              extract(epoch FROM ( COALESCE(lag(timestamp_utc) OVER (partition BY device_id ORDER BY timestamp_utc DESC), now()::timestamp) - timestamp_utc)) / 3600 AS delta_hours
                                     FROM     status.legacy_status_state_change lssc 
                                     WHERE    device_id = parent.device_id --and host_rcpn = parent.host_rcpn
                                     AND      timestamp_utc >= NOW() - INTERVAL '12 hours'
                      
                             ) p ) 
      SELECT   device_id, 
               last(host_rcpn, timestamp_utc) AS host_rcpn, 
               device_state, 
               max(timestamp_utc)  AS last_event_utc, 
               count(device_state) AS num_events, 
               sum(delta_hours)    AS total_hours_error 
      FROM     state_changes 
      WHERE    device_state BETWEEN x'7000'::int AND x'7FFF'::int 
      AND      ( 
                        next_state != device_state 
               OR       next_state IS NULL) 
      GROUP BY device_id, 
               device_state