import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, to_json, struct,
    when, current_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from configs import (
    TOPIC_SENSORS,
    TOPIC_TEMP_ALERTS,
    TOPIC_HUM_ALERTS,
    kafka_config,
    validate_kafka_config,
)

# Kafka configuration
KAFKA_SERVERS = ",".join(kafka_config["bootstrap_servers"])
KAFKA_SECURITY_PROTOCOL = kafka_config["security_protocol"]
KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
)

# Checkpoint paths for fault tolerance
CHECKPOINT_ROOT = os.getenv("SPARK_CHECKPOINT_ROOT", "/tmp/spark_checkpoint_alerts")
CHECKPOINT_MODE = os.getenv("SPARK_CHECKPOINT_MODE", "fresh").lower()
if CHECKPOINT_MODE == "fresh":
    _run_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    CHECKPOINT_BASE = f"{CHECKPOINT_ROOT}_{_run_suffix}"
else:
    CHECKPOINT_BASE = CHECKPOINT_ROOT

CHECKPOINT_TEMP = f"{CHECKPOINT_BASE}_temp"
CHECKPOINT_HUM = f"{CHECKPOINT_BASE}_hum"
ALERTS_CSV_PATH = str((Path(__file__).resolve().parent.parent / "data" / "alerts_conditions.csv"))

# Schema for sensor data messages
SENSOR_SCHEMA = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
])


def get_spark_session():
    """Initialize Spark Session with Kafka support"""
    return SparkSession.builder \
        .appName("IoT_Alert_Processing") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.jars.packages", KAFKA_PACKAGE) \
        .getOrCreate()


def get_kafka_stream_options():
    """Build Kafka options for Spark source/sink.

    Spark Structured Streaming expects Kafka client options with `kafka.` prefix.
    """
    options = {
        "kafka.bootstrap.servers": KAFKA_SERVERS,
        "kafka.security.protocol": KAFKA_SECURITY_PROTOCOL,
    }

    if KAFKA_SECURITY_PROTOCOL.upper().startswith("SASL"):
        options["kafka.sasl.mechanism"] = kafka_config["sasl_mechanism"]
        options["kafka.sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        )

    return options


def load_alert_conditions(spark):
    """Load alert conditions from CSV file"""
    df = spark.read.csv(ALERTS_CSV_PATH, header=True, inferSchema=True)
    
    # Replace -999 with NULL for conditions that shouldn't be applied
    df = df.withColumn("humidity_min", when(col("humidity_min") == -999, None).otherwise(col("humidity_min"))) \
           .withColumn("humidity_max", when(col("humidity_max") == -999, None).otherwise(col("humidity_max"))) \
           .withColumn("temperature_min", when(col("temperature_min") == -999, None).otherwise(col("temperature_min"))) \
           .withColumn("temperature_max", when(col("temperature_max") == -999, None).otherwise(col("temperature_max")))
    
    return df


def main():
    validate_kafka_config()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    kafka_stream_options = get_kafka_stream_options()
    
    print("=" * 60)
    print("IoT Alert Processing Pipeline")
    print("=" * 60)
    print(f"Kafka Servers: {KAFKA_SERVERS}")
    print(f"Input Topic: {TOPIC_SENSORS}")
    print(f"Output Topics: {TOPIC_TEMP_ALERTS}, {TOPIC_HUM_ALERTS}")
    print(f"Checkpoint mode: {CHECKPOINT_MODE}")
    print(f"Temp checkpoint: {CHECKPOINT_TEMP}")
    print(f"Hum checkpoint: {CHECKPOINT_HUM}")
    print("=" * 60)
    
    # Load alert conditions
    print("Loading alert conditions...")
    alert_conditions_df = load_alert_conditions(spark)
    alert_conditions_df.show(truncate=False)
    
    # Read from Kafka
    print(f"Connecting to Kafka topic: {TOPIC_SENSORS}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .options(**kafka_stream_options) \
        .option("subscribe", TOPIC_SENSORS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and extract timestamp as TimestampType
    sensor_df = kafka_df \
        .select(from_json(col("value").cast(StringType()), SENSOR_SCHEMA).alias("data")) \
        .select(
            col("data.sensor_id"),
            col("data.temperature"),
            col("data.humidity"),
            col("data.timestamp")
        )
    
    # Convert timestamp string to proper timestamp type for windowing
    sensor_df = sensor_df \
        .withColumn("timestamp_ts", col("timestamp").cast("timestamp"))
    
    # Apply windowing: 1 minute window, 30 second slide, 10 second watermark
    print("Applying windowing: 1 minute window with 30-second slides and 10-second watermark...")
    windowed_df = sensor_df \
        .withWatermark("timestamp_ts", "10 seconds") \
        .groupBy(window(col("timestamp_ts"), "1 minute", "30 seconds")) \
        .agg(
            avg(col("temperature")).alias("t_avg"),
            avg(col("humidity")).alias("h_avg")
        )
    
    # Add alert conditions via cross join
    alerts_df = windowed_df.crossJoin(alert_conditions_df)
    
    # Apply alert logic: alert triggers when values fall WITHIN the alert ranges
    alerts_df = alerts_df \
        .withColumn("temp_alert",
            when(col("temperature_min").isNull() & col("temperature_max").isNull(), False) \
            .when(col("temperature_min").isNull(), col("t_avg") <= col("temperature_max")) \
            .when(col("temperature_max").isNull(), col("t_avg") >= col("temperature_min")) \
            .otherwise((col("t_avg") >= col("temperature_min")) & (col("t_avg") <= col("temperature_max")))
        ) \
        .withColumn("humidity_alert",
            when(col("humidity_min").isNull() & col("humidity_max").isNull(), False) \
            .when(col("humidity_min").isNull(), col("h_avg") <= col("humidity_max")) \
            .when(col("humidity_max").isNull(), col("h_avg") >= col("humidity_min")) \
            .otherwise((col("h_avg") >= col("humidity_min")) & (col("h_avg") <= col("humidity_max")))
        ) \
        .filter(col("temp_alert") | col("humidity_alert"))  # Alert if temp OR humidity is in problematic range
    
    # Split alerts into temperature and humidity topics
    # Temperature alerts: codes 103 (cold), 104 (hot)
    temp_alerts_df = alerts_df \
        .filter(col("code").isin(103, 104)) \
        .select(
            struct(
                col("window.start").alias("start"),
                col("window.end").alias("end")
            ).alias("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp")
        ).select(
            to_json(struct("*")).alias("value")
        )
    
    # Humidity alerts: codes 101 (dry), 102 (wet)
    hum_alerts_df = alerts_df \
        .filter(col("code").isin(101, 102)) \
        .select(
            struct(
                col("window.start").alias("start"),
                col("window.end").alias("end")
            ).alias("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp")
        ).select(
            to_json(struct("*")).alias("value")
        )
    
    # Write to Kafka topics
    print(f"Writing temperature alerts to: {TOPIC_TEMP_ALERTS}")
    query_temp = temp_alerts_df \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .options(**kafka_stream_options) \
        .option("topic", TOPIC_TEMP_ALERTS) \
        .option("checkpointLocation", CHECKPOINT_TEMP) \
        .start()
    
    print(f"Writing humidity alerts to: {TOPIC_HUM_ALERTS}")
    query_hum = hum_alerts_df \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .options(**kafka_stream_options) \
        .option("topic", TOPIC_HUM_ALERTS) \
        .option("checkpointLocation", CHECKPOINT_HUM) \
        .start()
    
    print("\nStreaming started. Press Ctrl+C to stop...")
    print("=" * 60)
    
    # Wait for both queries to terminate
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
