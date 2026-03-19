import json
from kafka import KafkaConsumer, KafkaProducer

from configs import (
    TOPIC_HUM_ALERTS,
    TOPIC_SENSORS,
    TOPIC_TEMP_ALERTS,
    get_kafka_client_config,
    validate_kafka_config,
)


def build_consumer():
    return KafkaConsumer(
        TOPIC_SENSORS,
        **get_kafka_client_config(),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="building_sensors_processor_group",
    )


def build_producer():
    return KafkaProducer(
        **get_kafka_client_config(),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main():
    consumer = None
    producer = None

    try:
        validate_kafka_config()
        consumer = build_consumer()
        producer = build_producer()

        print(f"Обробник датчиків працює. Читання з: {TOPIC_SENSORS}")

        for message in consumer:
            data = message.value
            sensor_id = data.get("sensor_id")
            timestamp = data.get("timestamp")
            temperature = data.get("temperature")
            humidity = data.get("humidity")

            print(f"\nОтримано дані: {data}")

            if temperature is not None and temperature > 40:
                temp_alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "temperature": temperature,
                    "message": "Температура перевищує 40°C!",
                }
                producer.send(TOPIC_TEMP_ALERTS, value=temp_alert).get(timeout=10)
                print(
                    f"[УВАГА - ТЕМПЕРАТУРА] Відправлено сповіщення у "
                    f"{TOPIC_TEMP_ALERTS}: {temp_alert}"
                )

            if humidity is not None and (humidity > 80 or humidity < 20):
                hum_alert = {
                    "sensor_id": sensor_id,
                    "timestamp": timestamp,
                    "humidity": humidity,
                    "message": "Рівень вологості виходить за межі допустимого діапазону (20-80%)!",
                }
                producer.send(TOPIC_HUM_ALERTS, value=hum_alert).get(timeout=10)
                print(
                    f"[УВАГА - ВОЛОГІСТЬ] Відправлено сповіщення у "
                    f"{TOPIC_HUM_ALERTS}: {hum_alert}"
                )

    except KeyboardInterrupt:
        print("\nПроцесор зупинено користувачем.")
    except Exception as error:
        print(f"Сталася помилка: {error}")
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
