from kafka.admin import KafkaAdminClient, NewTopic

from configs import (
    TOPIC_HUM_ALERTS,
    TOPIC_SENSORS,
    TOPIC_TEMP_ALERTS,
    get_kafka_client_config,
    my_name,
    validate_kafka_config,
)


def main():
    admin_client = None

    try:
        validate_kafka_config()

        admin_client = KafkaAdminClient(
            **get_kafka_client_config(),
        )

        topics_to_create = [
            NewTopic(name=TOPIC_SENSORS, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_TEMP_ALERTS, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC_HUM_ALERTS, num_partitions=1, replication_factor=1),
        ]

        existing_topics = set(admin_client.list_topics())
        new_topics = [topic for topic in topics_to_create if topic.name not in existing_topics]

        if new_topics:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            print(f"Створено нові топіки: {[topic.name for topic in new_topics]}")
        else:
            print("Всі топіки вже існують.")

        print("\n----------------------")
        print("Список моїх топіків:")
        print("----------------------")
        for topic_name in admin_client.list_topics():
            if my_name in topic_name:
                print(topic_name)

    except Exception as error:
        print(f"Сталася помилка: {error}")
    finally:
        if admin_client is not None:
            admin_client.close()


if __name__ == "__main__":
    main()
