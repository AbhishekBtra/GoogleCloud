SELECT
  ARRAY
    (
        SELECT AS STRUCT 1, 2, 3
        UNION ALL 
        SELECT AS STRUCT 4, 5, 6
    ) AS new_array;

/*
Row	new_array._field_1	new_array._field_2	new_array._field_3	
1             	1					               2					                       3
	               4				  	            5					                        6
*/

SELECT ARRAY_CONCAT([1, 2], [3, 4], [5, 6]) as count_to_six;
--[1,2,3,4,5,6]

WITH items AS
  (
  SELECT ["coffee", NULL, "milk" ] as list
  UNION ALL
  SELECT ["cake", "pie"] as list
  )
SELECT 	ARRAY_TO_STRING(list, ', ', 'NULL'),
		ARRAY_LENGTH(list) AS size
FROM items
ORDER BY size DESC;
/*
Row	    str						                     size	
1	          coffee, NULL, milk		  3
2	          cake, pie				                2
*/

SELECT GENERATE_ARRAY(10, 0, -3) AS example_array;

/*
| example_array |
| [10, 7, 4, 1] |
*/

SELECT GENERATE_DATE_ARRAY('2016-10-05', '2016-10-08') AS example;
/*
+--------------------------------------------------+
| example                                          |
+--------------------------------------------------+
| [2016-10-05, 2016-10-06, 2016-10-07, 2016-10-08] |
+--------------------------------------------------+
*/