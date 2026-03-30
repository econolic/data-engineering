import os


DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092"
DEFAULT_USERNAME = ""
DEFAULT_PASSWORD = ""


def _split_servers(value):
    return [server.strip() for server in value.split(",") if server.strip()]


kafka_config = {
    "bootstrap_servers": _split_servers(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVER)
    ),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    "username": os.getenv("KAFKA_USERNAME", DEFAULT_USERNAME),
    "password": os.getenv("KAFKA_PASSWORD", DEFAULT_PASSWORD),
}

my_name = os.getenv("KAFKA_TOPIC_PREFIX", "max").strip() or "max"

TOPIC_SENSORS = f"{my_name}_sensors"
TOPIC_TEMP_ALERTS = f"{my_name}_temperature_alerts"
TOPIC_HUM_ALERTS = f"{my_name}_humidity_alerts"


def validate_kafka_config():
    if not kafka_config["bootstrap_servers"]:
        raise ValueError("Не задано KAFKA_BOOTSTRAP_SERVERS.")

    if kafka_config["security_protocol"].upper().startswith("SASL"):
        if not kafka_config["username"] or not kafka_config["password"]:
            raise ValueError(
                "Для SASL-підключення потрібно задати KAFKA_USERNAME та KAFKA_PASSWORD."
            )


def get_kafka_client_config():
    client_config = {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
    }

    if kafka_config["security_protocol"].upper().startswith("SASL"):
        client_config.update(
            {
                "sasl_mechanism": kafka_config["sasl_mechanism"],
                "sasl_plain_username": kafka_config["username"],
                "sasl_plain_password": kafka_config["password"],
            }
        )

    return client_config
