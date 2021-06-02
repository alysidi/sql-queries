WITH select_devices AS (
  SELECT device_id
  FROM status.device_shadow
  -- WHERE device_type = 'PVLINK'
	--WHERE st between x'7000'::int and x'7FFF'::int
	 -- WHERE device_id='0001000747CD'
  ORDER by timestamp_utc desc
  LIMIT 4000  -- TODO: limit is for testing only, remove later - need to batch 
),
state_changes AS (
  SELECT
   p.*
	from select_devices parent
	CROSS JOIN LATERAL (
			Select device_id,
			host_rcpn,
			device_type,
			timestamp_utc,
			st AS device_state,
			lead(st) over (PARTITION BY device_id ORDER BY timestamp_utc ASC) AS next_state,
			-- delta in hours between states. If no next state, use current time
			EXTRACT(EPOCH FROM (
				COALESCE(lead(timestamp_utc) over (PARTITION BY device_id ORDER BY timestamp_utc ASC),
						 now()::TIMESTAMP) - timestamp_utc)) / 3600
			  AS delta_hours
				 FROM status.legacy_status_state_change lssc
		  WHERE
			   device_id = parent.device_id
		AND timestamp_utc  between '2021-05-31' and now()
			-- limit to states only
			AND st between x'7000'::int and x'7FFF'::int
	   ) p
	  )
	   
SELECT
device_id,
  last(host_rcpn, timestamp_utc) AS host_rcpn,
  device_state,
  state_text,
  device_type,
   time_bucket(INTERVAL '1 day', timestamp_utc) AS day,
  COUNT(device_state) AS num_events,
   to_char(to_timestamp((SUM(delta_hours)) * 60), 'MI:SS') as total_duration_in_error
FROM state_changes sc LEFT JOIN status.rcp_state r ON device_state & x'FFF0'::int = r.state_code
WHERE
device_state between x'7000'::int and x'7FFF'::int AND
   (next_state != device_state or next_state is null)
GROUP BY day, device_id, device_type, device_state, state_text
ORDER BY device_id, day desc