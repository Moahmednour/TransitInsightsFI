from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType, ArrayType

# Define the schema of the Kafka messages
schema = StructType() \
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

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read the stream from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-kafka-kafka-1:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Extract the message value and parse JSON
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data"))

# Parse the message content
parsed_df = value_df.select(
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

# Display the processed data in the console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
