import argparse
from uuid import uuid4
from src.kafka_config import sasl_conf,schema_config
from six.moves import input
from src.kafka_logger import logging
from confluent_kafka import Producer 
from confluent_kafka.serialization import StringSerializer,SerializationContext,MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from typing import List
from src.entity.generic import Generic,instance_to_dict

FILE_PATH = "C://Users//Aditya//OneDrive//Documents//DataScience//projects//sensor-fault-detection-pipeline//sensor-fault-pipeline//sample_data//kafka-sensor-topic//aps_failure_training_set1.csv"

def sensor_to_dict(sensor:Generic, ctx):
    """
    Returns a dictionary representation of a user instance for serialization
    Args:
        user(User):User instance
        ctx(Serialization Context): Metadata pertaining to the serialization operation

    Returns:
        dict : Dictionary populated with user attributes to be serialized
    """

    return sensor.record


def delivery_report(err,msg):
    """
    Reports the status of message delivery(either success or failure)

    Args: 
        err(KafkaError) : The error that occured in case of failure to push the record to kafka
        msg(Message) : provides the status either the record/instance is successfully pushed to kafka or failed 
    """

    if err is not None:
        logging.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        #return
    logging.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(),msg.topic(),msg.partition(),msg.offset()))
    

def product_data_using_file(topic,file_path):
    logging.info(f"Topic: {topic} file_path:{file_path}")
    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str,schema_registry_client, instance_to_dict)
    producer = Producer(sasl_conf())

    print("Producing user records to topic {}".format(topic))

    producer.poll(0.0)
    try:
        for instance in Generic.get_object(file_path=file_path):
            print(instance)
            logging.info(f"Topic: {topic} file_path:{instance.to_dict()}")
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()),instance.to_dict()),
                             value=json_serializer(instance,SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            print("\nFlushing records...")
            producer.flush()
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...") 