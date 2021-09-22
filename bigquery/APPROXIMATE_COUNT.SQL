SELECT
  APPROX_COUNT_DISTINCT(x) AS approx_distinct --5
FROM
  UNNEST([0, 1, 1, 2, 3, 5]) AS x;