import json
from kafka import KafkaConsumer

from configs import (
    TOPIC_TEMP_ALERTS,
    TOPIC_HUM_ALERTS,
    get_kafka_client_config,
    validate_kafka_config,
)


def main():
    consumer = None

    try:
        validate_kafka_config()

        consumer = KafkaConsumer(
            TOPIC_TEMP_ALERTS,
            TOPIC_HUM_ALERTS,
            **get_kafka_client_config(),
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="alert_validation_group",
        )

        print("=" * 70)
        print("Alert Consumer - Monitoring Alerts")
        print("=" * 70)
        print(f"Listening to topics: {TOPIC_TEMP_ALERTS}, {TOPIC_HUM_ALERTS}")
        print("=" * 70)
        print()

        for message in consumer:
            topic = message.topic
            value = message.value
            offset = message.offset
            t_avg = value.get("t_avg")
            h_avg = value.get("h_avg")

            print(f"Topic: {topic} | Offset: {offset}")
            print(f"Alert Data:")
            print(f"  Window: {value.get('window', {})}")
            print(
                f"  Temperature Average: {t_avg:.2f}°C"
                if isinstance(t_avg, (int, float))
                else f"  Temperature Average: {t_avg}"
            )
            print(
                f"  Humidity Average: {h_avg:.2f}%"
                if isinstance(h_avg, (int, float))
                else f"  Humidity Average: {h_avg}"
            )
            print(f"  Code: {value.get('code')} | Message: {value.get('message')}")
            print(f"  Timestamp: {value.get('timestamp')}")
            print("-" * 70)

    except KeyboardInterrupt:
        print("\nКонсьюмер зупинено.")
    except Exception as error:
        print(f"Сталася помилка: {error}")
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()
