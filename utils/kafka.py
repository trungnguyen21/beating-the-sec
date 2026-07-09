from config import KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET

def kafka_client_config() -> dict:
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP,
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": KAFKA_API_KEY,
        "sasl_plain_password": KAFKA_API_SECRET,
    }