from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType, NullType

# Define the schema of the Kafka messages
schema = StructType() \
    .add("topic", StringType()) \
    .add("message", StringType())

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read the stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-kafka-kafka-1:9092") \
    .option("subscribe", "mqtt-data") \
    .load()

# Extract the message value and parse JSON
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.message")

# Define a more detailed schema for the message content
message_schema = StructType() \
    .add("VP", StructType() \
         .add("desi", StringType()) \
         .add("dir", StringType()) \
         .add("oper", IntegerType()) \
         .add("veh", IntegerType()) \
         .add("tst", StringType()) \
         .add("tsi", LongType()) \
         .add("spd", DoubleType()) \
         .add("hdg", IntegerType()) \
         .add("lat", DoubleType()) \
         .add("long", DoubleType()) \
         .add("acc", DoubleType()) \
         .add("dl", IntegerType()) \
         .add("odo", DoubleType()) \
         .add("drst", IntegerType()) \
         .add("oday", StringType()) \
         .add("jrn", IntegerType()) \
         .add("line", IntegerType()) \
         .add("start", StringType()) \
         .add("loc", StringType()) \
         .add("stop", NullType()) \
         .add("route", StringType()) \
         .add("occu", IntegerType()))

# Parse the message content
parsed_df = value_df.withColumn("message_json", from_json(col("message"), message_schema)) \
    .select("message_json.VP.desi", "message_json.VP.dir", "message_json.VP.oper", \
            "message_json.VP.veh", "message_json.VP.tst", "message_json.VP.tsi", "message_json.VP.spd", \
            "message_json.VP.hdg", "message_json.VP.lat", "message_json.VP.long", "message_json.VP.acc", \
            "message_json.VP.dl", "message_json.VP.odo", "message_json.VP.drst", "message_json.VP.oday", \
            "message_json.VP.jrn", "message_json.VP.line", "message_json.VP.start", "message_json.VP.loc", \
            "message_json.VP.route", "message_json.VP.occu")

# Rename columns for better readability
parsed_df = parsed_df.withColumnRenamed("desi", "destination") \
    .withColumnRenamed("dir", "direction") \
    .withColumnRenamed("oper", "operator") \
    .withColumnRenamed("veh", "vehicle_id") \
    .withColumnRenamed("tst", "timestamp") \
    .withColumnRenamed("tsi", "timestamp_int") \
    .withColumnRenamed("spd", "speed") \
    .withColumnRenamed("hdg", "heading") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("long", "longitude") \
    .withColumnRenamed("acc", "acceleration") \
    .withColumnRenamed("dl", "delay") \
    .withColumnRenamed("odo", "odometer") \
    .withColumnRenamed("drst", "drst_status") \
    .withColumnRenamed("oday", "operating_day") \
    .withColumnRenamed("jrn", "journey_number") \
    .withColumnRenamed("line", "line_number") \
    .withColumnRenamed("start", "start_time") \
    .withColumnRenamed("loc", "location_source") \
    .withColumnRenamed("route", "route_id") \
    .withColumnRenamed("occu", "occupancy")

# Display the processed data in the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
