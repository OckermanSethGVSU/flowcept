import mochi.mofka.client as mofka
from mochi.mofka.client import ThreadPool, AdaptiveBatchSize
import json
import os
import time
import csv
print("about to start", flush=True)
driver = mofka.MofkaDriver("mofka.json")
batch_size = 1024
thread_pool = ThreadPool(0)
# create a topic
topic_name = "interception"
topic = driver.open_topic(topic_name)
consumer_name = "flowcept"
consumer = topic.consumer(name=consumer_name,
                            thread_pool=thread_pool,
                            batch_size=batch_size)

pulls = []
events = []
count = 0


# Get the list of files in the current directory
csv_files = [file for file in os.listdir() if file.endswith('.csv')]
threshold = len(csv_files)
print("about to start with breakpoint ",threshold, flush=True)
while True:
    t1 = time.time()
    f = consumer.pull()
    event = f.wait()
    t2 = time.time()
    pulls.append(t2 - t1)
    e = json.loads(event.metadata)

    # events.append(e)
    # print("h: ", e.keys(),flush=True)
    
    # break

    if "type" in e.keys() and 'info' in e.keys():
        if e['type'] == 'flowcept_control':
            if e['info'] == "mq_dao_thread_stopped":
                print(e,flush=True)
                count += 1
        
    if count == threshold:
        # print("About to write", flush=True)
        # # with open("data.json", 'w') as f:
        # #     json.dump(events, f, indent=4)
        # with open("pulls.csv", "w", newline="") as f:
        #     writer = csv.writer(f)
        #     writer.writerow(pulls)
        print("Done :) ", flush=True)
        break


    

    
    