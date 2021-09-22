SELECT 
		APPROX_TOP_COUNT(x, 2) as approx_top_count
FROM 	
UNNEST(
  	["apple", "apple", "pear", "pear", "pear", "banana"]
) as x;
/*
Row	approx_top_count.value	approx_top_count.count	
1	    pear					                    3
         apple					                    2
*/