SELECT
  prt.name,
  CAST(SUM(prt_splits)AS float64) AS sum_of_splits
FROM
  `data-to-insights.advanced.race_results`,
  UNNEST(participants) AS prt,
  UNNEST(splits) AS prt_splits
GROUP BY
  prt.name


/*
SCHEMA
Field name          Type             Mode
-------------          -------------     -------------
race	                STRING	     NULLABLE	
participants	    RECORD	    REPEATED	
    name	          STRING	    NULLABLE	
    splits	             FLOAT	       REPEATED	
-------------------------------------------------------
*/

/*
INPUT
-------------------------------------------------------
RACE   participants.NAME    participants.SPLITS
800M    Rudisha
                                                    23.4
                                                    26.3
                                                    26.4
                                                    26.1
            Makhloufi
                                                    24.5
                                                    25.4
                                                    26.6
                                                    26.1
            Murphy
                                                    23.9
                                                    26.0
                                                    27.0
                                                    26.0
            Bosse
                                                    23.6
                                                    26.2
                                                    26.5
                                                    27.1
            Rotich
                                                    24.7
                                                    25.6
                                                    26.9
                                                    26.4
            Lewandowski
                                                    25.0
                                                    25.7
                                                    26.3
                                                    27.2
            Kipketer
                                                    23.2
                                                    26.1
                                                    27.3
                                                    29.4
            Berian
                                                    23.7
                                                    26.1
                                                    27.0
                                                    29.3
*/

  /*
  OUTPUT
  -------------------------------------------------------
  Row	name	sum_of_splits	
1	
Rudisha
102.19999999999999
2	
Makhloufi
102.6
3	
Murphy
102.9
4	
Bosse
103.4
5	
Rotich
103.6
6	
Lewandowski
104.2
7	
Kipketer
106.0
8	
Berian
106.1
-------------------------------------------------------
*/