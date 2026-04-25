"""
Фінальний проєкт — Частина 1.
Building an End-to-End Streaming Pipeline.

Пайплайн:
  1. Зчитування athlete_bio з MySQL
  2. Фільтрація невалідних height/weight
  3. Зчитування athlete_event_results з MySQL → запис у Kafka → читання стрімом
  4. Join streaming events з bio за athlete_id
  5. Агрегація avg(height, weight) по sport/medal/sex/country_noc + timestamp
  6a. forEachBatch → вихідний Kafka-топік
  6b. forEachBatch → MySQL таблиця
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, avg, current_timestamp, lit, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# ---------------------------------------------------------------------------
# Завантаження .env
# ---------------------------------------------------------------------------

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

# MySQL (JDBC)
MYSQL_HOST = os.getenv("MYSQL_HOST", "217.61.57.46")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB", "olympic_dataset")
MYSQL_USER = os.getenv("MYSQL_USER", "neo_data_admin")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"

# Kafka
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "77.81.230.104:9092")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_USER = os.getenv("KAFKA_USER", "admin")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")
KAFKA_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
)

# Топіки Kafka
TOPIC_INPUT = os.getenv("TOPIC_INPUT", "maxim_athlete_event_results")
TOPIC_OUTPUT = os.getenv("TOPIC_OUTPUT", "maxim_enriched_athlete_avg")

# Таблиця для результатів
OUTPUT_TABLE = os.getenv("OUTPUT_TABLE", "maxim_enriched_athlete_avg")

# Шлях до JDBC драйвера
JAR_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql-connector-j-8.0.32.jar")

# Spark-Kafka пакет
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

# ---------------------------------------------------------------------------
# Допоміжні функції
# ---------------------------------------------------------------------------

def get_kafka_options():
    """Повертає словник опцій Kafka для Spark readStream/writeStream."""
    return {
        "kafka.bootstrap.servers": KAFKA_SERVER,
        "kafka.security.protocol": KAFKA_SECURITY_PROTOCOL,
        "kafka.sasl.mechanism": KAFKA_SASL_MECHANISM,
        "kafka.sasl.jaas.config": KAFKA_JAAS_CONFIG,
    }

# ---------------------------------------------------------------------------
# Головний пайплайн
# ---------------------------------------------------------------------------

def main():
    # Створення Spark сесії
    spark = (
        SparkSession.builder
        .appName("FP_StreamingPipeline")
        .config("spark.jars", JAR_PATH)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_opts = get_kafka_options()

    # ===========================================================================
    # Етап 1: Зчитування athlete_bio з MySQL
    # ===========================================================================
    print("=" * 60)
    print("Етап 1: Зчитування athlete_bio з MySQL")
    print("=" * 60)

    athlete_bio_df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", "athlete_bio")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )
    print(f"athlete_bio — кількість рядків: {athlete_bio_df.count()}")
    athlete_bio_df.show(5)

    # ===========================================================================
    # Етап 2: Фільтрація невалідних height/weight
    # ===========================================================================
    print("=" * 60)
    print("Етап 2: Фільтрація невалідних height/weight")
    print("=" * 60)

    # Clean numeric fields: replace comma with dot and cast safely to double
    athlete_bio_filtered = (
        athlete_bio_df
        .withColumn("height_clean", regexp_replace(col("height"), ",", "."))
        .withColumn("weight_clean", regexp_replace(col("weight"), ",", "."))
        .withColumn("height_double", col("height_clean").cast("double"))
        .withColumn("weight_double", col("weight_clean").cast("double"))
        .filter(
            col("height").isNotNull()
            & col("weight").isNotNull()
            & (col("height") != "")
            & (col("weight") != "")
            & col("height_double").isNotNull()
            & col("weight_double").isNotNull()
        )
        .withColumn("height", col("height_double"))
        .withColumn("weight", col("weight_double"))
        .drop("height_clean", "weight_clean", "height_double", "weight_double")
    )
    print(f"athlete_bio (після фільтрації) — кількість рядків: {athlete_bio_filtered.count()}")
    athlete_bio_filtered.show(5)

    # ===========================================================================
    # Етап 3: Зчитування athlete_event_results з MySQL → запис у Kafka → читання стрімом
    # ===========================================================================
    print("=" * 60)
    print("Етап 3: MySQL → Kafka → Spark readStream")
    print("=" * 60)

    # 3.1: Зчитування athlete_event_results з MySQL
    athlete_events_df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", "athlete_event_results")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )
    print(f"athlete_event_results — кількість рядків: {athlete_events_df.count()}")
    athlete_events_df.show(5)

    # 3.2: Запис у Kafka-топік (batch write)
    print(f"Запис athlete_event_results у Kafka топік: {TOPIC_INPUT}")
    (
        athlete_events_df
        .select(to_json(struct("*")).alias("value"))
        .write
        .format("kafka")
        .options(**kafka_opts)
        .option("topic", TOPIC_INPUT)
        .save()
    )
    print("Дані успішно записано в Kafka.")

    # 3.3: Визначення схеми для парсингу JSON з Kafka
    # Отримуємо схему з уже зчитаного DataFrame
    event_schema = athlete_events_df.schema

    # 3.4: Читання стрімом з Kafka
    print(f"Читання стрімом з Kafka топіку: {TOPIC_INPUT}")
    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .option("subscribe", TOPIC_INPUT)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )

    # 3.5: Парсинг JSON у DataFrame-формат
    events_stream_df = (
        kafka_stream_df
        .select(from_json(col("value").cast("string"), event_schema).alias("data"))
        .select("data.*")
    )

    # ===========================================================================
    # Етап 4: Join streaming events з bio за athlete_id
    # ===========================================================================
    print("=" * 60)
    print("Етап 4: Join streaming events з bio за athlete_id")
    print("=" * 60)

    enriched_df = events_stream_df.join(
        athlete_bio_filtered.drop("country_noc"),
        on="athlete_id",
        how="inner"
    )

    # ===========================================================================
    # Етап 5: Агрегація avg(height, weight) по sport/medal/sex/country_noc + timestamp
    # ===========================================================================
    print("=" * 60)
    print("Етап 5: Агрегація avg(height, weight)")
    print("=" * 60)

    agg_df = (
        enriched_df
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    # ===========================================================================
    # Етап 6: forEachBatch — запис у Kafka-топік та MySQL
    # ===========================================================================
    print("=" * 60)
    print("Етап 6: forEachBatch → Kafka + MySQL")
    print("=" * 60)

    def foreach_batch_function(batch_df, batch_id):
        """Обробляє кожний мікробатч: пише у Kafka та MySQL."""
        if batch_df.isEmpty():
            return

        # Показуємо дані батчу
        print(f"\n--- Batch {batch_id} ---")
        batch_df.show(truncate=False)

        # Етап 6.а): запис у вихідний Kafka-топік
        (
            batch_df
            .select(to_json(struct("*")).alias("value"))
            .write
            .format("kafka")
            .options(**kafka_opts)
            .option("topic", TOPIC_OUTPUT)
            .save()
        )
        print(f"Batch {batch_id}: записано у Kafka топік '{TOPIC_OUTPUT}'")

        # Етап 6.b): запис у базу даних MySQL
        (
            batch_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", JDBC_DRIVER)
            .option("dbtable", OUTPUT_TABLE)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id}: записано у MySQL таблицю '{OUTPUT_TABLE}'")

    # Запуск стриму
    query = (
        agg_df
        .writeStream
        .foreachBatch(foreach_batch_function)
        .outputMode("complete")
        .start()
    )

    print("\nСтрим запущено. Очікування завершення...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
