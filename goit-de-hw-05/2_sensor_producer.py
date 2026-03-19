import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

from configs import TOPIC_SENSORS, get_kafka_client_config, validate_kafka_config


def build_producer():
    return KafkaProducer(
        **get_kafka_client_config(),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main():
    producer = None
    sensor_id = random.randint(1000, 9999)

    try:
        validate_kafka_config()
        producer = build_producer()

        print(f"Запущено датчик з ID: {sensor_id}")
        print(f"Відправка даних у топік: {TOPIC_SENSORS}")

        while True:
            data = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "temperature": round(random.uniform(25, 45), 2),
                "humidity": round(random.uniform(15, 85), 2),
            }

            future = producer.send(TOPIC_SENSORS, value=data)
            future.get(timeout=10)

            print(f"Відправлено: {data}")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nРоботу датчика зупинено.")
    except Exception as error:
        print(f"Сталася помилка: {error}")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
