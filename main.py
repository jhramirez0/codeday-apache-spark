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

# * Define Data Schemas
taxi_schema = StructType([
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

zone_schema = StructType([
    StructField('LocationID', IntegerType(), True),
    StructField('Borough', StringType(), True),
    StructField('Zone', StringType(), True),
    StructField('service_zone', StringType(), True),
])

# * Create Spark SQL DataFrame
taxiDF = spark.read.csv(
    path='resources/data/yellow_tripdata_2020-*.csv.gz',
    schema=taxi_schema,
    sep=',',
    header=True,
)

zoneDF = spark.read.csv(
    path='resources/lookup_data/taxi+_zone_lookup.csv',
    schema=zone_schema,
    sep=",",
    header=True,
)

# * Create In-Memory View
taxiDF.createOrReplaceTempView("TAXI")
zoneDF.createOrReplaceTempView("ZONE")

# * Custom Spark SQL
queryDF = spark.sql("""
    SELECT
        CASE 
            WHEN TAXI.VendorID = 1 THEN 'Creative Mobile Technologies'
            WHEN TAXI.VendorID = 2 THEN 'VeriFone Inc.'
        ELSE 'Other' END AS vendor_name,
        ZONE.Borough as pickup_location,
        CAST(SUM(TAXI.total_amount) AS decimal(18,2)) as total_revenue

    FROM TAXI
    INNER JOIN ZONE ON ZONE.LocationID = TAXI.PULocationID
    WHERE total_amount > 0
    GROUP BY (CASE 
            WHEN TAXI.VendorID = 1 THEN 'Creative Mobile Technologies'
            WHEN TAXI.VendorID = 2 THEN 'VeriFone Inc.'
        ELSE 'Other' END), ZONE.Borough
        
    ORDER BY (CASE 
            WHEN TAXI.VendorID = 1 THEN 'Creative Mobile Technologies'
            WHEN TAXI.VendorID = 2 THEN 'VeriFone Inc.'
        ELSE 'Other' END), ZONE.Borough
""")

queryDF.show()