from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import config


def create_producer(config_file: str, key_schema: str = None, value_schema: str = None):

    schema_registry_raw_config = config.read_config(config_file, 'Schema Registry Configuration')

    schema_registry_config = {
        'url': schema_registry_raw_config['schema.registry.url'],
        'basic.auth.user.info': schema_registry_raw_config['basic.auth.user.info']}

    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    producer_config = config.read_config(config_file, 'Kafka Configuration')

    if key_schema is not None:
        producer_config['key.serializer'] = AvroSerializer(schema_registry_client, key_schema)

    if value_schema is not None:
        producer_config['value.serializer'] = AvroSerializer(schema_registry_client, value_schema)

    return SerializingProducer(producer_config)

