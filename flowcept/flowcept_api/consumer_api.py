from typing import List, Union
from time import sleep

from flowcept.commons.daos.mq_dao import MQDao
from flowcept.flowceptor.consumers.document_inserter import DocumentInserter
from flowcept.commons.flowcept_logger import FlowceptLogger
from flowcept.flowceptor.adapters.base_interceptor import BaseInterceptor


# TODO: :code-reorg: This may not be considered an API anymore as it's doing critical things for the good functioning of the system.
class FlowceptConsumerAPI(object):
    def __init__(
        self,
        interceptors: Union[BaseInterceptor, List[BaseInterceptor]] = None,
        bundle_exec_id = None,
        start_doc_inserter=True
    ):
        self.logger = FlowceptLogger()
        self._document_inserter: DocumentInserter = None
        self._mq_dao = MQDao()
        self._start_doc_inserter = start_doc_inserter
        if bundle_exec_id is None:
            self._bundle_exec_id = id(self)
        else:
            self._bundle_exec_id = bundle_exec_id
        if interceptors is not None and type(interceptors) != list:
            interceptors = [interceptors]
        self._interceptors: List[BaseInterceptor] = interceptors
        self.is_started = False

    def start(self):
        if self.is_started:
            self.logger.warning("Consumer is already started!")
            return self

        if self._interceptors and len(self._interceptors):
            for interceptor in self._interceptors:
                # TODO: :base-interceptor-refactor: revise
                if interceptor.settings is None:
                    key = id(interceptor)
                else:
                    key = interceptor.settings.key
                self.logger.debug(f"Flowceptor {key} starting...")
                interceptor.start(bundle_exec_id=self._bundle_exec_id)
                self.logger.debug(f"...Flowceptor {key} started ok!")

        if self._start_doc_inserter:
            self.logger.debug("Flowcept Consumer starting...")
            self._document_inserter = DocumentInserter(
                check_safe_stops=True,
                bundle_exec_id=self._bundle_exec_id
            ).start()
            # sleep(1)
            self.logger.debug("Ok, we're consuming messages!")
        self.is_started = True
        return self

    def stop(self):
        if not self.is_started:
            self.logger.warning("Consumer is already stopped!")
            return
        sleep_time = 1
        self.logger.info(
            f"Received the stop signal. We're going to wait {sleep_time} secs."
            f" before gracefully stopping..."
        )
        sleep(sleep_time)
        if self._interceptors and len(self._interceptors):
            for interceptor in self._interceptors:
                # TODO: :base-interceptor-refactor: revise
                if interceptor.settings is None:
                    key = id(interceptor)
                else:
                    key = interceptor.settings.key
                self.logger.info(f"Flowceptor {key} stopping...")
                interceptor.stop()
                self.logger.info("... ok!")
        if self._start_doc_inserter:
            self.logger.info("Stopping Doc Inserter...")
            self._document_inserter.stop(bundle_exec_id=self._bundle_exec_id)
        self.is_started = False
        self.logger.debug("All stopped!")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
