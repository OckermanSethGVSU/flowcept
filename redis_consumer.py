import redis
import msgpack
from flowcept.configs import MQ_HOST, MQ_PORT, MQ_CHANNEL
import json
import os
import time
import csv

# Connect to Redis
redis_client = redis.Redis(host=MQ_HOST, port=MQ_PORT, db=0)
pubsub = redis_client.pubsub()
pubsub.subscribe(MQ_CHANNEL)

with open("sub.txt", "w") as f:
    f.write("")

    
print("Subscribed... the consumer is going to sleep... zzz", flush=True)
while not os.path.isfile("redis_consumer_flag.txt"):
    time.sleep(60)
    print("Not found; back to sleep... zzz", flush=True)

print("Consumer woke up", flush=True)


csv_files = [file for file in os.listdir() if file.endswith('.csv')]
threshold = len(csv_files)
print("about to start with breakpoint ",threshold, flush=True)

pulls = []
events = []
count = 0
while True:
    t1 = time.time()
    message = pubsub.get_message(timeout=10)
    t2 = time.time()
    pulls.append(t2 - t1)
   
    if isinstance(message["data"], int):
        message["data"] = str(message["data"]).encode()  # Convert to string and encode to bytes

    unpacked_data = msgpack.loads(message["data"], strict_map_key=False)
    if isinstance(unpacked_data, dict):
        # Proceed with processing the dictionary
        e = unpacked_data
        events.append(e)
        if "type" in e.keys() and 'info' in e.keys():
            if e['type'] == 'flowcept_control':
                if e['info'] == "mq_dao_thread_stopped":
                    print(e,flush=True)
                    count += 1
        
        if count == threshold:
            with open("data.json", 'w') as f:
                json.dump(events, f, indent=4)
            with open("pulls.csv", "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(pulls)
            print("Done :) ", flush=True)
            
            break
    
    
    pulls.append(t2 - t1)
    
with open("writeoutComplete.txt", "w") as f:
    f.write("yay")

