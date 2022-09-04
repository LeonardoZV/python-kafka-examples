import argparse
import configparser
from uuid import uuid4


class User(object):
    """
        Name stores the deserialized Avro record for the Kafka key.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "id"]

    def __init__(self, name=None):
        self.name = name
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    @staticmethod
    def dict_to_user(obj, ctx):
        return User(obj['name'])

    @staticmethod
    def user_to_dict(name, ctx):
        return User.to_dict(name)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(name=self.name)


def parse_cli_args():

    parser = argparse.ArgumentParser()

    required = parser.add_argument_group('required arguments')

    required.add_argument('-c',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)

    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)

    args = parser.parse_args()

    return args


def parse_api_args():

    parser = argparse.ArgumentParser()

    required = parser.add_argument_group('required arguments')

    required.add_argument('-c',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)

    args = parser.parse_args()

    return args


def read_config(config_file, section):
    config = configparser.ConfigParser()
    config.read(config_file)
    if section is None:
        return dict(config.items())
    else:
        return dict(config[section].items())
