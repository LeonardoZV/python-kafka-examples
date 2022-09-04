from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

import config


def create_consumer(config_file: str, key_schema: str = None, value_schema: str = None):

    schema_registry_raw_config = config.read_config(config_file, 'Schema Registry Configuration')

    schema_registry_config = {
        'url': schema_registry_raw_config['schema.registry.url'],
        'basic.auth.user.info': schema_registry_raw_config['basic.auth.user.info']}

    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    consumer_config = config.read_config(config_file, 'Kafka Configuration')

    if key_schema is None:
        consumer_config['key.deserializer'] = AvroDeserializer(schema_registry_client)
    else:
        consumer_config['key.deserializer'] = AvroDeserializer(schema_registry_client, key_schema)

    if value_schema is None:
        consumer_config['value.deserializer'] = AvroDeserializer(schema_registry_client)
    else:
        consumer_config['value.deserializer'] = AvroDeserializer(schema_registry_client, value_schema)

    consumer_config['group.id'] = 'python_example_group_2'

    consumer_config['auto.offset.reset'] = 'earliest'

    return DeserializingConsumer(consumer_config)