
import time
import pathlib
import logging
import json,yaml
import itertools
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)

class KafkaClient():
    def __init__(self,server: str,port: str,topic: str,additional_producer_config: dict):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers = [f"{server}:{port}"],
            **additional_producer_config
        )

    def send(self,data):
        self.producer.send(
            self.topic,
            data
        ).add_callback(KafkaClient.on_send_success).add_errback(KafkaClient.on_send_error)

    def block_and_wait(self,seconds):
        logging.info(f"blocking for {seconds} seconds")
        time.sleep(seconds)
        logging.info("blocking the producer untill all the asynchronous sends are actually done")
        self.producer.flush()
        

    @staticmethod
    def on_send_success(record_metadata):
        logging.info(
            f"""
            TOPIC: {record_metadata.topic}
            PARTITION: {record_metadata.partition}
            OFFSET: {record_metadata.offset}
            """
        )
    @staticmethod
    def on_send_error(e):
        logging.error(
            f"{e}"
        )

def read_file_yaml_to_dict(path):
    with open(path,'r') as f:
        data = yaml.safe_load(f)
        return data

def read_text_file_by_line(path):
    with open(path,'r') as f:
        data = f.readlines()
        return data

def add_timestamp(data):
    data["@timestamp"] = f'{datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]}Z'
    return data

def main():

    DATA_PATH = pathlib.Path(__file__).parent.parent.resolve()/"messages"/"messages.json"
    CONFIG_PATH = pathlib.Path(__file__).parent.parent.resolve()/"config"/"config.yml"
    CONFIG = read_file_yaml_to_dict(CONFIG_PATH)

    BROKER = CONFIG.get("basic").get("broker")
    PORT = CONFIG.get("basic").get("port")
    TOPIC = CONFIG.get("basic").get("topic")
    THROUGHPUT = CONFIG.get("basic").get("throughput")
    WAIT = CONFIG.get("basic").get("wait")

    ADDITIONAL_PRODUCER_CONFIG = CONFIG.get("additional-kafka-producer-config")

    _data = [json.loads(line) for line in read_text_file_by_line(DATA_PATH)]
    _continuous_data = itertools.cycle(_data)
    _producer = KafkaClient(BROKER,PORT,TOPIC,ADDITIONAL_PRODUCER_CONFIG)

    _wait_threshold = 0

    for data in _continuous_data:
        _payload = json.dumps(add_timestamp(data)).encode("utf-8")
        _producer.send(_payload)

        _wait_threshold+=1

        if(_wait_threshold == THROUGHPUT):
            _producer.block_and_wait(WAIT)
            _wait_threshold = 0


if __name__ == "__main__":
    main()