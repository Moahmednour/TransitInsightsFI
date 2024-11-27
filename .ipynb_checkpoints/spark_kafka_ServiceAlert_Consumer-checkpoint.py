from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, from_unixtime, lit, coalesce
from pyspark.sql.types import StructType, StringType, ArrayType, LongType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Kafka_Improved_ServiceAlerts") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka settings
kafka_broker = "spark-kafka-kafka-1:9092"
service_alerts_topic = "service-alerts"

# Read the stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", service_alerts_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Define schema for service-alerts
service_alerts_schema = StructType() \
    .add("active_period", ArrayType(StructType()
         .add("start", LongType(), True)
         .add("end", LongType(), True))) \
    .add("informed_entity", ArrayType(StructType()
         .add("agency_id", StringType(), True)
         .add("route_id", StringType(), True)
         .add("stop_id", StringType(), True))) \
    .add("header_text", StringType(), True) \
    .add("description_text", StringType(), True)

# Parse the service-alerts data
service_alerts_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), service_alerts_schema).alias("data"))

# Explode and select fields
service_alerts_df = service_alerts_df \
    .select(
        explode(col("data.active_period")).alias("active_period"),
        col("data.informed_entity"),
        col("data.header_text"),
        col("data.description_text")
    ) \
    .select(
        col("active_period.start").alias("active_start"),
        col("active_period.end").alias("active_end"),
        explode(col("informed_entity")).alias("informed_entity"),
        col("header_text"),
        col("description_text")
    ) \
    .select(
        from_unixtime(col("active_start"), "yyyy-MM-dd HH:mm:ss").alias("active_start"),
        from_unixtime(col("active_end"), "yyyy-MM-dd HH:mm:ss").alias("active_end"),
        coalesce(col("informed_entity.agency_id"), lit("N/A")).alias("agency_id"),
        coalesce(col("informed_entity.route_id"), lit("N/A")).alias("route_id"),
        coalesce(col("informed_entity.stop_id"), lit("N/A")).alias("stop_id"),
        coalesce(col("header_text"), lit("No Header")).alias("header_text"),
        coalesce(col("description_text"), lit("No Description")).alias("description_text")
    )

# Write the cleaned and formatted data to the console for debugging
query = service_alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
