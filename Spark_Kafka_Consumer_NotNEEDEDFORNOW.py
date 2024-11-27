from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType, ArrayType, NullType

# Define schemas for different Kafka messages
weather_schema = StructType() \
    .add("coord", StructType() \
         .add("lon", DoubleType()) \
         .add("lat", DoubleType())) \
    .add("weather", ArrayType(StructType() \
         .add("id", IntegerType()) \
         .add("main", StringType()) \
         .add("description", StringType()) \
         .add("icon", StringType()))) \
    .add("base", StringType()) \
    .add("main", StructType() \
         .add("temp", DoubleType()) \
         .add("feels_like", DoubleType()) \
         .add("temp_min", DoubleType()) \
         .add("temp_max", DoubleType()) \
         .add("pressure", IntegerType()) \
         .add("humidity", IntegerType()) \
         .add("sea_level", IntegerType()) \
         .add("grnd_level", IntegerType())) \
    .add("visibility", IntegerType()) \
    .add("wind", StructType() \
         .add("speed", DoubleType()) \
         .add("deg", IntegerType()) \
         .add("gust", DoubleType())) \
    .add("clouds", StructType() \
         .add("all", IntegerType())) \
    .add("dt", LongType()) \
    .add("sys", StructType() \
         .add("type", IntegerType()) \
         .add("id", IntegerType()) \
         .add("country", StringType()) \
         .add("sunrise", LongType()) \
         .add("sunset", LongType())) \
    .add("timezone", IntegerType()) \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("cod", IntegerType())

mqtt_schema = StructType() \
    .add("topic", StringType()) \
    .add("message", StringType())

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

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read the stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-kafka-kafka-1:9092") \
    .option("subscribe", "weather-data,mqtt-data") \
    .load()

# Extract the message value and topic
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string", "topic")

# Apply schema based on topic
weather_df = value_df.filter(col("topic") == "weather-data") \
    .select(from_json(col("json_string"), weather_schema).alias("data")) \
    .select(
        col("data.coord.lon").alias("longitude"),
        col("data.coord.lat").alias("latitude"),
        col("data.weather").getItem(0).getField("main").alias("weather_main"),
        col("data.weather").getItem(0).getField("description").alias("weather_description"),
        col("data.main.temp").alias("temperature"),
        col("data.main.feels_like").alias("feels_like_temperature"),
        col("data.main.pressure").alias("pressure"),
        col("data.main.humidity").alias("humidity"),
        col("data.visibility").alias("visibility"),
        col("data.wind.speed").alias("wind_speed"),
        col("data.wind.deg").alias("wind_direction"),
        col("data.clouds.all").alias("cloud_coverage"),
        col("data.dt").alias("timestamp"),
        col("data.sys.country").alias("country"),
        col("data.name").alias("city_name")
    )

mqtt_df = value_df.filter(col("topic") == "mqtt-data") \
    .select(from_json(col("json_string"), mqtt_schema).alias("data")) \
    .select(from_json(col("data.message"), message_schema).alias("message_json")) \
    .select(
        col("message_json.VP.desi").alias("destination"),
        col("message_json.VP.dir").alias("direction"),
        col("message_json.VP.oper").alias("operator"),
        col("message_json.VP.veh").alias("vehicle_id"),
        col("message_json.VP.tst").alias("timestamp"),
        col("message_json.VP.tsi").alias("timestamp_int"),
        col("message_json.VP.spd").alias("speed"),
        col("message_json.VP.hdg").alias("heading"),
        col("message_json.VP.lat").alias("latitude"),
        col("message_json.VP.long").alias("longitude"),
        col("message_json.VP.acc").alias("acceleration"),
        col("message_json.VP.dl").alias("delay"),
        col("message_json.VP.odo").alias("odometer"),
        col("message_json.VP.drst").alias("drst_status"),
        col("message_json.VP.oday").alias("operating_day"),
        col("message_json.VP.jrn").alias("journey_number"),
        col("message_json.VP.line").alias("line_number"),
        col("message_json.VP.start").alias("start_time"),
        col("message_json.VP.loc").alias("location_source"),
        col("message_json.VP.route").alias("route_id"),
        col("message_json.VP.occu").alias("occupancy")
    )

# Union the two DataFrames
final_df = weather_df.unionByName(mqtt_df, allowMissingColumns=True)

# Display the processed data in the console
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
