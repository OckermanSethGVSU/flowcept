import unittest
from threading import Thread
from time import sleep
from uuid import uuid4
import numpy as np

from dask.distributed import Client

from flowcept.commons.doc_db.document_db_dao import DocumentDBDao
from flowcept.commons.doc_db.document_inserter import (
    DocumentInserter,
)
from flowcept.commons.flowcept_logger import FlowceptLogger


def dummy_func1(x, workflow_id=None):
    return x * 2


def dummy_func2(y, workflow_id=None):
    return y + y


def dummy_func3(z, w, workflow_id=None):
    return {"r": z + w}


def dummy_func4(x_obj, workflow_id=None):
    return {"z": x_obj["x"] * 2}


def forced_error_func(x):
    raise Exception(f"This is a forced error: {x}")


class TestDask(unittest.TestCase):
    client: Client = None
    consumer_thread: Thread = None

    def __init__(self, *args, **kwargs):
        super(TestDask, self).__init__(*args, **kwargs)
        self.logger = FlowceptLogger().get_logger()

    @staticmethod
    def _init_consumption():
        TestDask.consumer_thread = Thread(
            target=DocumentInserter().main, daemon=True
        ).start()
        sleep(3)

    @classmethod
    def setUpClass(cls):
        TestDask.client = TestDask._setup_local_dask_cluster()
        TestDask.consumer_thread = None

    @staticmethod
    def _setup_local_dask_cluster():
        from dask.distributed import Client, LocalCluster
        from flowcept import (
            FlowceptDaskSchedulerPlugin,
            FlowceptDaskWorkerPlugin,
        )

        cluster = LocalCluster(n_workers=2)
        scheduler = cluster.scheduler
        client = Client(scheduler.address)

        # Instantiate and Register FlowceptPlugins, which are the ONLY
        # additional steps users would need to do in their code:
        scheduler_plugin = FlowceptDaskSchedulerPlugin(scheduler)
        scheduler.add_plugin(scheduler_plugin)

        worker_plugin = FlowceptDaskWorkerPlugin()
        client.register_worker_plugin(worker_plugin)

        return client

    def test_pure_workflow(self):
        i1 = np.random.random()
        wf_id = f"wf_{uuid4()}"
        o1 = self.client.submit(dummy_func1, i1, workflow_id=wf_id)
        o2 = TestDask.client.submit(dummy_func2, o1, workflow_id=wf_id)
        self.logger.debug(o2.result())
        self.logger.debug(o2.key)
        sleep(10)
        return o2.key

    def test_long_workflow(self):
        i1 = np.random.random()
        wf_id = f"wf_{uuid4()}"
        o1 = TestDask.client.submit(dummy_func1, i1, workflow_id=wf_id)
        o2 = TestDask.client.submit(dummy_func2, o1, workflow_id=wf_id)
        o3 = TestDask.client.submit(dummy_func3, o1, o2, workflow_id=wf_id)
        self.logger.debug(o3.result())
        sleep(10)
        return o3.key

    def varying_args(self):
        i1 = np.random.random()
        o1 = TestDask.client.submit(dummy_func3, i1, w=2)
        result = o1.result()
        assert result["r"] > 0
        self.logger.debug(result)
        self.logger.debug(o1.key)
        sleep(10)
        return o1.key

    def test_map_workflow(self):
        i1 = np.random.random(3)
        wf_id = f"wf_{uuid4()}"
        o1 = TestDask.client.map(dummy_func1, i1, workflow_id=wf_id)
        for o in o1:
            result = o.result()
            assert result > 0
            self.logger.debug(f"{o.key}, {result}")
        sleep(10)
        return o1

    def test_map_workflow_kwargs(self):
        i1 = [
            {"x": np.random.random(), "y": np.random.random()},
            {"x": np.random.random()},
        ]
        wf_id = f"wf_{uuid4()}"
        o1 = TestDask.client.map(dummy_func4, i1, workflow_id=wf_id)
        for o in o1:
            result = o.result()
            assert result["z"] > 0
            self.logger.debug(o.key, result)
        sleep(10)
        return o1

    def error_task_submission(self):
        i1 = np.random.random()
        o1 = TestDask.client.submit(forced_error_func, i1)
        try:
            self.logger.debug(o1.result())
        except:
            pass
        return o1.key

    def test_observer_and_consumption(self):
        doc_dao = DocumentDBDao()
        if TestDask.consumer_thread is None:
            TestDask._init_consumption()
        o2_task_id = self.test_pure_workflow()
        sleep(10)
        assert len(doc_dao.find({"task_id": o2_task_id})) > 0

    def test_observer_and_consumption_varying_args(self):
        doc_dao = DocumentDBDao()
        if TestDask.consumer_thread is None:
            TestDask._init_consumption()
        o2_task_id = self.varying_args()
        sleep(10)
        assert len(doc_dao.find({"task_id": o2_task_id})) > 0

    def test_observer_and_consumption_error_task(self):
        doc_dao = DocumentDBDao()
        if TestDask.consumer_thread is None:
            TestDask._init_consumption()
        o2_task_id = self.error_task_submission()
        sleep(10)
        docs = doc_dao.find({"task_id": o2_task_id})
        assert len(docs) > 0
        assert docs[0]["stderr"]["exception"]
