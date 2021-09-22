SELECT APPROX_TOP_SUM(x, weight, 2) AS approx_top_sum 
FROM UNNEST(
  [
  STRUCT("apple" AS x, 3 AS weight),
  ("pear", 2),
  ("apple", 0),
  ("banana", 5),
  ("pear", 4)
]);
/*
Row	approx_top_sum.value	approx_top_sum.sum	
1	    pear 					                6
	    banana					                5
*/