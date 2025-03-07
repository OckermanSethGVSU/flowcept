"""MQ redis module."""

from typing import Callable

import msgpack
from time import time
import csv

from flowcept.commons.daos.mq_dao.mq_dao_base import MQDao
from flowcept.commons.utils import perf_log
from flowcept.configs import (
    MQ_CHANNEL,
    PERF_LOG,
)


class MQDaoRedis(MQDao):
    """MQ redis class."""

    MESSAGE_TYPES_IGNORE = {"psubscribe"}

    def __init__(self, kv_host=None, kv_port=None, adapter_settings=None):
        super().__init__(kv_host, kv_port, adapter_settings)
        self._producer = self._kv_conn  # if MQ is redis, we use the same KV for the MQ
        self._consumer = None
        self.flush_events = []
        

    def subscribe(self):
        """
        Subscribe to interception channel.
        """
        self._consumer = self._kv_conn.pubsub()
        self._consumer.psubscribe(MQ_CHANNEL)

    def message_listener(self, message_handler: Callable):
        """Get message listener."""
        for message in self._consumer.listen():
            self.logger.debug("Received a message!")
            if message["type"] in MQDaoRedis.MESSAGE_TYPES_IGNORE:
                continue
            msg_obj = msgpack.loads(
                message["data"],
                strict_map_key=False,  # , cls=DocumentInserter.DECODER
            )
            if not message_handler(msg_obj):
                break

    def send_message(self, message: dict, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        """Send the message."""
        t1 = time()
        self._producer.publish(channel, serializer(message))
        t2 = time()
        self.flush_events.append(["single",t1,t2,t2 - t1, len(str(message).encode())])

    def _bulk_publish(self, buffer, channel=MQ_CHANNEL, serializer=msgpack.dumps):
        total = 0
        pipe = self._producer.pipeline()
        for message in buffer:
            try:
                total += len(str(message).encode())
                pipe.publish(MQ_CHANNEL, serializer(message))
            except Exception as e:
                self.logger.exception(e)
                self.logger.error("Some messages couldn't be flushed! Check the messages' contents!")
                self.logger.error(f"Message that caused error: {message}")
        t0 = 0
        if PERF_LOG:
            t0 = time()
        try:
            t1 = time()
            pipe.execute()
            t2 = time()
            self.flush_events.append(["bulk", t1,t2,t2 - t1,total])
            self.logger.debug(f"Flushed {len(buffer)} msgs to MQ!")
        except Exception as e:
            self.logger.exception(e)
        perf_log("mq_pipe_execute", t0)

    def liveness_test(self):
        """Get the livelyness of it."""
        try:
            super().liveness_test()
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def stop(self,interceptor_instance_id: str, bundle_exec_id: int = None):
        t1 = time()
        super().stop(interceptor_instance_id, bundle_exec_id)
        t2 = time()
        self.flush_events.append(["final", t1, t2, t2 - t1,'n/a'])

        
        with open(f"redis_{interceptor_instance_id}_redis_flush_events.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["type", "start","end","duration","size"])
            writer.writerows(self.flush_events)
        
        # lets consumer know when to stop
        self.producer.push(metadata={"message":{"stop-now"}})  # using metadata to send data
        self.producer.flush()