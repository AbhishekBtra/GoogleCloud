SELECT
  CONCAT('Count Of < 50 is ',FORMAT('%t',COUNTIF(x<50))) AS count_less_than_50
FROM
  UNNEST([45,98,12, 23, 50]) AS x 

/*
  INPUT
  -----------------------------------------------------------------------------------------------------------
  [45,98,12,23,50]

OUTPUT
  "Count Of < 50 is 3"
*/