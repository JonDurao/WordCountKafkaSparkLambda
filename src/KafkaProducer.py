import base64
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import BrokerConnection
import socket
import os

# sudoPassword = '181631'
# command = 'mount -t vboxsf kafka /home/kafka/'
# p = os.system('echo %s|sudo -S %s' % (sudoPassword, command))
#
# os.system('sudo /home/kafka/bin/zookeeper-server-start.sh -daemon /home/kafka/config/zookeeper.properties')
# os.system('sudo /home/kafka/bin/kafka-server-start.sh -daemon /home/kafka/config/server.properties')

with open("../cervantes.txt", "r") as f:
    cervantes_file = f.readlines()

cervantes_file = [x.strip() for x in cervantes_file]

broker = BrokerConnection('localhost', 9092, socket.AF_INET)
broker.connect_blocking()
print(broker.connecting())

# To consume latest messages and auto-commit offsets
producer = KafkaProducer(bootstrap_servers='localhost:9092')

while not broker.connected():
    print(123)

assert broker.connected() is True

count = 0
for x in cervantes_file:
    time.sleep(0.5)
    y = x.encode()
    print(y)
    count += 1
    producer.send('test-topic', y)
