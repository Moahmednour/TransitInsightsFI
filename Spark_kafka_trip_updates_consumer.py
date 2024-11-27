from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, from_unixtime, coalesce, lit
from pyspark.sql.types import StructType, StringType, ArrayType, LongType

# Create Spark session with increased memory
spark = SparkSession.builder \
    .appName("Kafka_TripUpdates_Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Kafka settings
kafka_broker = "spark-kafka-kafka-1:9092"
trip_updates_topic = "trip-updates"

# Read the stream from Kafka with required options only
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", trip_updates_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# Deserialize key and value as strings
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define schema for trip-updates
trip_updates_schema = StructType() \
    .add("trip_id", StringType()) \
    .add("route_id", StringType()) \
    .add("start_time", StringType()) \
    .add("start_date", StringType()) \
    .add("stop_time_updates", ArrayType(StructType()
         .add("stop_id", StringType())
         .add("arrival_time", LongType())
         .add("departure_time", LongType())))

# Parse the trip-updates data
trip_updates_df = kafka_df.select(from_json(col("value"), trip_updates_schema).alias("data"))

# Explode and process data
processed_trip_updates_df = trip_updates_df \
    .select(
        col("data.trip_id").alias("Trip ID"),
        col("data.route_id").alias("Route ID"),
        col("data.start_time").alias("Start Time"),
        col("data.start_date").alias("Start Date"),
        explode(col("data.stop_time_updates")).alias("Stop Update")
    ) \
    .select(
        col("Trip ID"),
        col("Route ID"),
        col("Start Time"),
        col("Start Date"),
        col("Stop Update.stop_id").alias("Stop ID"),
        from_unixtime(col("Stop Update.arrival_time"), "yyyy-MM-dd HH:mm:ss").alias("Arrival Time"),
        from_unixtime(col("Stop Update.departure_time"), "yyyy-MM-dd HH:mm:ss").alias("Departure Time")
    ) \
    .select(
        coalesce(col("Trip ID"), lit("N/A")).alias("Trip ID"),
        coalesce(col("Route ID"), lit("N/A")).alias("Route ID"),
        coalesce(col("Start Time"), lit("N/A")).alias("Start Time"),
        coalesce(col("Start Date"), lit("N/A")).alias("Start Date"),
        coalesce(col("Stop ID"), lit("N/A")).alias("Stop ID"),
        coalesce(col("Arrival Time"), lit("N/A")).alias("Arrival Time"),
        coalesce(col("Departure Time"), lit("N/A")).alias("Departure Time")
    )

# Write the processed data to the console with checkpointing
query = processed_trip_updates_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_trip_updates") \
    .start()

query.awaitTermination()
