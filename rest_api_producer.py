import json
from typing import Union

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

import config
from producer_factory import create_producer

app = FastAPI()

producers = {}


class KafkaProduceRequestModel(BaseModel):
    topic: str
    headers: Union[dict, None] = None
    key: Union[dict, None] = None
    key_schema: Union[dict, None] = None
    value: Union[dict, None] = None
    value_schema: Union[dict, None] = None


@app.post("/produce")
def produce(request: KafkaProduceRequestModel):

    args = config.parse_cli_args()

    producer_key = []

    if request.key_schema is None:
        producer_key.append("None")
    else:
        producer_key.append("{namespace}.{name}".format(namespace=request.key_schema.get("namespace"),
                                                        name=request.key_schema.get("name")))

    producer_key.append("-")

    if request.value_schema is None:
        producer_key.append("None")
    else:
        producer_key.append("{namespace}.{name}".format(namespace=request.value_schema.get("namespace"),
                                                        name=request.value_schema.get("name")))

    producer_key_str = "".join(producer_key)

    producer = producers.get(producer_key_str, None)

    if producer is None:
        key_schema_str = None if request.key_schema is None else json.dumps(request.key_schema)
        value_schema_str = None if request.value_schema is None else json.dumps(request.value_schema)
        producer = create_producer(args.config_file, key_schema_str, value_schema_str)
        producers[producer_key_str] = producer

    producer.produce(topic=request.topic, value=request.value)

    producer.poll(0)

    return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
