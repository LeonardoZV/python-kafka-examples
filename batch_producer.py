import json

import config
from producer_factory import create_producer

user_schema = """
    {
        "namespace": "example.avro",
        "name": "User",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"}
        ]
    }
"""

if __name__ == '__main__':

    args = config.parse_cli_args()

    producer = create_producer(args.config_file, None, user_schema)

    delivered_records = 0

    def acked(err, msg):
        global delivered_records

        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1

            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    for n in range(1):
        user_string = "{ \"name\" : \"Leo\"}"
        user = json.loads(user_string)
        print("Producing Avro record: {}".format(user['name']))
        producer.produce(topic=args.topic, value=user, on_delivery=acked)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, args.topic))
