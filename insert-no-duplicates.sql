

  CREATE TABLE tblOrder(
    OrderID     INT,
    CustomerID  INT,
    OrderTypeID INT,
    ts  timestamp,
    "nameplate" jsonb DEFAULT '{}'::jsonb
)


INSERT INTO tblOrder(CustomerID,OrderID,OrderTypeID,nameplate) VALUES
(1, 1, 45584565,'{"ah_rtg": 60.9, "wh_rtg": 17100, "w_cha_max": 9828, "w_discha_max": 9828, "soc_max": 100.0, "soc_min": 2.0, "soc_rsv_max": 100.0, "soc_rsv_min": 5.0}')




INSERT INTO tblOrder( CustomerID, orderid, ordertypeid,nameplate, ts)
SELECT * FROM (SELECT 5,1,455845652,'{"venkatesh":"213", "ah_rtg": 70,"some":"ok","aly":1}'::jsonb, now()) AS tmp
WHERE NOT EXISTS (
    SELECT 1 FROM tblOrder WHERE CustomerID = 5 and OrderID = 1 and ordertypeid=455845652
    and nameplate='{"aly":1, "ah_rtg": 70, "some":"ok",    "venkatesh":"213"}'
) LIMIT 1;


select customerid, max(ts) from tblOrder
where customerid=5
group by customerid


select * from tblOrder
where customerid=5

select * from status.device_shadow join status.nameplate on rcpn and join status.ess_device_info on rcpn






