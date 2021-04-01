
SELECT id, 
    TUMBLE_START(t, INTERVAL '1' minute) as wStart,
    TUMBLE_END(t, INTERVAL '1' minute) as wEnd,
    SUM(vc) sum_vc
FROM sensor
GROUP BY TUMBLE(t, INTERVAL '1' minute), id

SELECT id,
  hop_start(t, INTERVAL '1' minute, INTERVAL '1' hour) as wStart,
  hop_end(t, INTERVAL '1' minute, INTERVAL '1' hour) as wEnd,
  SUM(vc) sum_vc
FROM sensor
GROUP BY hop(t, INTERVAL '1' minute, INTERVAL '1' hour), id