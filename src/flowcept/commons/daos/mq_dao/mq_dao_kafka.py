"""MQ kafka module."""

from typing import Callable

import msgpack
import csv
from time import time

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient

from flowcept.commons.daos.mq_dao.mq_dao_base import MQDao
from flowcept.commons.utils import perf_log
from flowcept.configs import (
    MQ_CHANNEL,
    PERF_LOG,
    MQ_HOST,
    MQ_PORT,
)


class MQDaoKafka(MQDao):
    """MQ kafka class."""

    def __init__(self, adapter_settings=None):
        super().__init__(adapter_settings)

        self._kafka_conf = {
            "bootstrap.servers": f"{MQ_HOST}:{MQ_PORT}",
        }
        self._producer = Producer(self._kafka_conf)
        self._consumer = None
        self.flush_events = []

    def subscribe(self):
        """Subscribe to the interception channel."""
        self._kafka_conf.update(
            {
                "group.id": "my_group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        self._consumer = Consumer(self._kafka_conf)
        self._consumer.subscribe([MQ_CHANNEL])

    def message_listener(self, message_handler: Callable):
        """Get message listener."""
        try:
            while True:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break
                message = msgpack.loads(msg.value(), raw=False, strict_map_key=False)
                self.logger.debug(f"Received message: {message}")
                if not message_handler(message):
                    break
        except Exception as e:
            self.logger.exception(e)
        finally:
            self._consumer.close()

    def send_message(self, message: dict, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        """Send the message."""
        self._producer.produce(channel, key=channel, value=serializer(message))
        t1 = time()
        self._producer.flush()
        t2 = time()
        self.flush_events.append(["single",t1,t2,t2 - t1, len(str(message).encode())])

    def _bulk_publish(self, buffer, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        total = 0
        for message in buffer:
            try:
                self.logger.debug(f"Going to send Message:\n\t[BEGIN_MSG]{message}\n[END_MSG]\t")
                self._producer.produce(channel, key=channel, value=serializer(message))
                total += len(str(message).encode())
            except Exception as e:
                self.logger.exception(e)
                self.logger.error("Some messages couldn't be flushed! Check the messages' contents!")
                self.logger.error(f"Message that caused error: {message}")
        t0 = 0
        if PERF_LOG:
            t0 = time()
        try:
            t1 = time()
            self._producer.flush()
            t2 = time()
            self.flush_events.append(["bulk", t1,t2,t2 - t1,total])

            self.logger.info(f"Flushed {len(buffer)} msgs to MQ!")
        except Exception as e:
            self.logger.exception(e)
        perf_log("mq_pipe_flush", t0)

    def liveness_test(self):
        """Get the livelyness of it."""
        try:
            super().liveness_test()
            admin_client = AdminClient(self._kafka_conf)
            kafka_metadata = admin_client.list_topics(timeout=5)
            return MQ_CHANNEL in kafka_metadata.topics
        except Exception as e:
            self.logger.exception(e)
            return False
    
    def stop(self,interceptor_instance_id: str, bundle_exec_id: int = None):
        t1 = time()
        super().stop(interceptor_instance_id, bundle_exec_id)
        t2 = time()
        self.flush_events.append(["final", t1, t2, t2 - t1,'n/a'])

        
        with open(f"kafka_{interceptor_instance_id}_flush_events.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["type", "start","end","duration","size"])
            writer.writerows(self.flush_events)
        
        # lets consumer know when to stop
        
        self._producer.produce(MQ_CHANNEL, key=MQ_CHANNEL, value=msgpack.dumps({"message":"stop-now"}))  # using metadata to send data
        self._producer.flush()
