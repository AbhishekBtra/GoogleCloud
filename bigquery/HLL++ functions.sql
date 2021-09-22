SELECT
  hll_count.
MERGE
  (sktches),
  block_id
FROM (
  SELECT
    block_id,
    HLL_COUNT.INIT(tr.transaction_id) AS sktches
  FROM
    `bigquery-public-data`.bitcoin_blockchain.blocks tab,
    UNNEST(transactions) AS tr
  GROUP BY
    block_id )
GROUP BY
  block_id