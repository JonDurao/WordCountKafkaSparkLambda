import base64
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import BrokerConnection
import socket
import os

consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
for msg in consumer:
    print(msg)