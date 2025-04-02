from confluent_kafka import Consumer, KafkaException
import msgpack
from flowcept.configs import MQ_HOST, MQ_PORT, MQ_CHANNEL  # Assuming these are set for Kafka
import time
import os
import csv
import json

# Kafka configuration
conf = {
    "bootstrap.servers": f"{MQ_HOST}:{MQ_PORT}",
    "group.id": "my_group",
    "auto.offset.reset": "earliest"
}

# Create Kafka consumer
consumer = Consumer(conf)
consumer.subscribe([MQ_CHANNEL])

print("Listening for messages...")
csv_files = [file for file in os.listdir() if file.endswith('.csv')]
threshold = len(csv_files)
print("about to start with breakpoint ",threshold, flush=True)

pulls = []
events = []
count = 0

try:
    while True:
        t1 = time.time()
        msg = consumer.poll()  # Poll for new messages
        t2 = time.time()
        
        pulls.append(t2 - t1)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"Kafka error: {msg.error()}")
                break
        
        msg_data = msg.value()
        # print(msg_data,flush=True)
        
        if isinstance(msg_data, int):
            msg_data = str(msg_data).encode()  # Convert to string and encode to bytes
        
        msg_obj = msgpack.loads(msg_data, strict_map_key=False)
        
        if isinstance(msg_obj, dict):
            # Proceed with processing the dictionary
            e = msg_obj
            # events.append(e)
            if "type" in e.keys() and 'info' in e.keys():
                if e['type'] == 'flowcept_control':
                    if e['info'] == "mq_dao_thread_stopped":
                        print(e,flush=True)
                        count += 1
            
            if count == threshold:
                print("writing to file", flush=True)
                # with open("data.json", 'w') as f:
                #     json.dump(events, f, indent=4)
                with open("pulls.csv", "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(pulls)
                print("Done :) ", flush=True)
                
                break
    
    
   
finally:
    consumer.close()