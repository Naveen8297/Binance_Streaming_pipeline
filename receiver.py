

import socket
import json
import re
from kafka import KafkaProducer

def start_server(host, port, topic):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])   # To send to Kafka server running at 9092

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, port))
        sock.listen()
        print(f'Server listening on {host}:{port}')
        while True:
            conn, addr = sock.accept()
            print(f'Connected by {addr}')
            with conn:
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    received_data = data.decode('utf-8')
                    received = json.dumps(received_data)
                    rec = re.findall(r'\{.*?\}', received)
                    for r in rec:
                        producer.send(topic, r.encode('utf-8'))
                        print(f'Sent message to Kafka topic {topic}: {r}')
                        
start_server('localhost', 9093, 'stream')  # Gather data from 9093 and send to Kafka topic at 9092

