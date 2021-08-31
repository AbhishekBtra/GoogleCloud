from pyspark.sql import SparkSession

PROJECT_ID = 'gcp-big-data-292014'
DATASET = 'ecommerce'
TABLE_PRODUCTS = 'products'
TABLE_ALL_SESSION = 'all_sessions_raw'
FACT_TABLE = 'Session_Facts'


spark = SparkSession\
    .builder\
    .master('local')\
    .appName('creating_fact_table')\
    .getOrCreate()


sc = spark.sparkContext
sc.setLogLevel('ERROR')


temp_bucket = "dataproc-temp-us-west1-626447966509-e2jpq9pc"
spark.conf.set('temporaryGcsBucket',temp_bucket)

products = spark.read.format('bigquery')\
    .option('table', '.'.join([PROJECT_ID, DATASET, TABLE_PRODUCTS]))\
    .load()

all_sessions = spark.read.format('bigquery')\
    .option('table','.'.join([PROJECT_ID, DATASET, TABLE_ALL_SESSION]))\
    .load()


products.createOrReplaceTempView('temp_products')
all_sessions.createOrReplaceTempView('temp_sesions')

sql_query = """ SELECT    
                          temp_sesions.fullVisitorId, 
                          temp_sesions.country,
                          temp_sesions.city,
                          temp_sesions.date, 
                          temp_sesions.visitId, 
                          temp_sesions.productQuantity, 
                          temp_sesions.productSKU,
                          temp_sesions.v2ProductName, 
                          temp_sesions.v2ProductCategory,
                          temp_products.stockLevel, 
                          temp_products.sentimentScore
                FROM 
                temp_sesions
                LEFT JOIN temp_products
                ON temp_sesions.productSKU = temp_products.SKU
                """

fact_table = spark.sql(sqlQuery=sql_query)

fact_table.show(n=10)

fact_table.write.format('bigquery')\
    .option('table','.'.join([PROJECT_ID, DATASET, FACT_TABLE]))\
    .save()
