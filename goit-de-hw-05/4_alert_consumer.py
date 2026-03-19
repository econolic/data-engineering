import json
from kafka import KafkaConsumer

from configs import (
    TOPIC_HUM_ALERTS,
    TOPIC_TEMP_ALERTS,
    get_kafka_client_config,
    validate_kafka_config,
)


def build_consumer():
    return KafkaConsumer(
        TOPIC_TEMP_ALERTS,
        TOPIC_HUM_ALERTS,
        **get_kafka_client_config(),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="alerts_output_group",
    )


def main():
    consumer = None

    try:
        validate_kafka_config()
        consumer = build_consumer()

        print("Остаточні дані. Очікування сповіщень...")

        for message in consumer:
            topic = message.topic
            value = message.value

            if topic == TOPIC_TEMP_ALERTS:
                print("\n[АЛЕРТ ТЕМПЕРАТУРИ]")
                print(f"[{value['timestamp']}] Sensor ID: {value['sensor_id']}")
                print(f"Показники: Температура = {value['temperature']}°C")
                print(f"Повідомлення: {value['message']}")
                print("---------------------------------------")
            elif topic == TOPIC_HUM_ALERTS:
                print("\n[АЛЕРТ ВОЛОГОСТІ]")
                print(f"[{value['timestamp']}] Sensor ID: {value['sensor_id']}")
                print(f"Показники: Вологість = {value['humidity']}%")
                print(f"Повідомлення: {value['message']}")
                print("---------------------------------------")

    except KeyboardInterrupt:
        print("\nВивід сповіщень зупинено.")
    except Exception as error:
        print(f"Сталася помилка: {error}")
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()
