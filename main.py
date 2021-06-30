from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType
)

# * Build Spark Session
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.host","127.0.0.1") \
    .config("spark.driver.bindAddress","127.0.0.1") \
    .appName('CodeDay Demo') \
    .getOrCreate()

# * Define CSV Schema
schema = StructType([
    StructField('VendorID', IntegerType(), True),
    StructField('tpep_pickup_datetime', DateType(), True),
    StructField('tpep_dropoff_datetime', DateType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('RateCodeID', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True)
])

# * Create Spark SQL DataFrame
rawDF = spark.read.csv(
    path='resources/data/yellow_tripdata_2020-*.csv.gz',
    schema=schema,
    sep=',',
    header=True,
)

# * Create In-Memory View
rawDF.createOrReplaceTempView("TAXI")

# * Custom Spark SQL
queryDF = spark.sql("""
    SELECT
        CASE WHEN VendorID = 1 THEN 'Vendor_1' ELSE 'Vendor_2' END AS vendor_name,
        CAST(SUM(total_amount) AS decimal(18,2)) as total_revenue 
    FROM TAXI
    WHERE total_amount > 0
    GROUP BY CASE WHEN VendorID = 1 THEN 'Vendor_1' ELSE 'Vendor_2' END
""")

queryDF.show()