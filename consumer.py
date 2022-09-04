import json

from confluent_kafka.avro import SerializerError

import config
from consumer_factory import create_consumer

if __name__ == '__main__':

    args = config.parse_cli_args()

    consumer = create_consumer(args.config_file)

    consumer.subscribe([args.topic])

    while True:

        try:

            msg = consumer.poll(1.0)

            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue

            elif msg.error():
                print('error: {}'.format(msg.error()))

            else:
                deserialized_headers = \
                    None if msg.headers() is None else [(h[0], h[1].decode('utf-8')) for h in msg.headers()]
                print("Key {} | Headers {} | Value {}"
                      .format(json.dumps(msg.key()), json.dumps(deserialized_headers), json.dumps(msg.value())))

        except KeyboardInterrupt:
            break

        except SerializerError as e:
            print("Message deserialization failed {}".format(e))
            pass

    # Leave group and commit final offsets
    consumer.close()
