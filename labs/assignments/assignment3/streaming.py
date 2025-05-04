from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    from_json,
    count,
    avg,
    lag,
    desc,
    to_json,
    struct,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)
from pyspark.sql.window import Window

# 1) Create SparkSession
spark = (
    SparkSession.builder.appName("TrafficMonitoring")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2) Read from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic_data")
    .option("startingOffsets", "latest")
    .load()
)

# 3) Define schema and parse JSON
schema = StructType(
    [
        StructField("sensor_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("vehicle_count", IntegerType(), True),
        StructField("average_speed", FloatType(), True),
        StructField("congestion_level", StringType(), True),
    ]
)

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*").withColumn(
    "timestamp", col("timestamp").cast(TimestampType())
)

# 4) Data quality checks with watermark
clean_df = (
    events_df.filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull())
    .filter(col("vehicle_count") >= 0)
    .filter(col("average_speed") > 0)
    .dropDuplicates(["sensor_id", "timestamp"])
    .withWatermark("timestamp", "10 minutes")
)

# 5) Define analyses
# Traffic Volume (5-minute window)
volume_df = clean_df.groupBy(
    window(col("timestamp"), "5 minutes"), col("sensor_id")
).agg(count("vehicle_count").alias("total_vehicles"))

# Congestion Hotspots (cumulative count of HIGH events)
congestion_df = (
    clean_df.filter(col("congestion_level") == "HIGH")
    .groupBy("sensor_id")
    .agg(count("*").alias("total_high"))
    .filter(col("total_high") >= 3)
)

# Average Speed (10-minute window)
speed_df = clean_df.groupBy(
    window(col("timestamp"), "10 minutes"), col("sensor_id")
).agg(avg("average_speed").alias("avg_speed"))

# Sudden Speed Drops (2-minute window)
speed_drop_df = (
    clean_df.groupBy(window(col("timestamp"), "2 minutes"), col("sensor_id"))
    .agg(avg("average_speed").alias("avg_speed"))
    .filter(col("avg_speed") < 20.0)
)  # Threshold for "speed drop"

# Busiest Sensors (30-minute window, no ranking)
busiest_df = clean_df.groupBy(
    window(col("timestamp"), "30 minutes").alias("window"), col("sensor_id")
).agg(count("vehicle_count").alias("total_vehicles"))


# 6) Function to write DataFrame to Kafka
def write_to_kafka(df, topic, mode):
    return (
        df.select(to_json(struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", topic)
        .option("checkpointLocation", f"checkpoint/{topic}")
        .outputMode(mode)
        .trigger(processingTime="10 seconds")
        .start()
    )


# 7) Start streaming queries
volume_query = write_to_kafka(volume_df, "traffic_volume", "append")
congestion_query = write_to_kafka(congestion_df, "congestion_hotspots", "complete")
speed_query = write_to_kafka(speed_df, "average_speed", "append")
speed_drop_query = write_to_kafka(speed_drop_df, "speed_drops", "append")
busiest_query = write_to_kafka(busiest_df, "busiest_sensors", "append")

# 8) Await termination for all queries
spark.streams.awaitAnyTermination()
