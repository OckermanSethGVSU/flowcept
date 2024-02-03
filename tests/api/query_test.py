import os.path
import pathlib
import unittest
import json
import random
from threading import Thread
import requests
import inspect
from time import sleep
from uuid import uuid4
from datetime import datetime, timedelta

from flowcept.commons.flowcept_dataclasses.task_message import (
    TaskMessage,
    Status,
)
from flowcept.configs import WEBSERVER_PORT, WEBSERVER_HOST
from flowcept.flowcept_api.task_query_api import TaskQueryAPI
from flowcept.flowcept_webserver.app import app, BASE_ROUTE
from flowcept.flowcept_webserver.resources.query_rsrc import TaskQuery
from flowcept.commons.daos.document_db_dao import DocumentDBDao
from flowcept.analytics.analytics_utils import clean_telemetry_dataframe


def gen_some_mock_multi_workflow_data(size=1):
    """
    Generates a multi-workflow composed of two workflows.
    :param size: Maximum number of tasks to generate. The actual maximum will be 2*size because this mock data has two workflows.
    :return:
    """
    new_docs = []
    new_task_ids = []

    _end = datetime.now()

    for i in range(0, size):
        t1 = TaskMessage()
        t1.task_id = str(uuid4())
        t1.workflow_name = "generate_hyperparams"
        t1.workflow_id = t1.workflow_name + str(uuid4())
        t1.adapter_id = "adapter1"
        t1.used = {"ifile": "/path/a.dat"}
        t1.activity_id = "generate"
        t1.generated = {
            "epochs": random.randint(1, 100),
            "batch_size": random.randint(16, 20),
        }

        _start = _end + timedelta(minutes=i)
        _end = _start + timedelta(minutes=i + 1)

        t1.started_at = int(_start.timestamp())
        t1.ended_at = int(_end.timestamp())
        t1.campaign_id = "mock_campaign"
        t1.status = Status.FINISHED.name
        t1.user = "user_test"
        new_docs.append(t1.to_dict())
        new_task_ids.append(t1.task_id)

        t2 = TaskMessage()
        t2.task_id = str(uuid4())
        t1.adapter_id = "adapter2"
        t2.workflow_name = "train"
        t2.activity_id = "fit"
        t2.workflow_id = t2.workflow_name + str(uuid4())
        t2.used = t1.generated
        t2.generated = {
            "loss": random.uniform(0.5, 50),
            "accuracy": random.uniform(0.5, 0.95),
        }

        _start = _end + timedelta(minutes=i)
        _end = _start + timedelta(minutes=i + 1)

        t2.started_at = int(_start.timestamp())
        t2.ended_at = int(_end.timestamp())
        t2.status = Status.FINISHED.name
        t2.campaign_id = t1.campaign_id
        t2.user = t1.campaign_id
        new_docs.append(t2.to_dict())
        new_task_ids.append(t2.task_id)

    return new_docs, new_task_ids


def gen_some_mock_data(size=1, with_telemetry=False):
    if with_telemetry:
        fname = "sample_data_with_telemetry.json"
    else:
        fname = "sample_data.json"

    fpath = os.path.join(pathlib.Path(__file__).parent.resolve(), fname)
    with open(fpath) as f:
        docs = json.load(f)

    i = 0
    new_docs = []
    new_task_ids = []
    _end = datetime.now()
    for doc in docs:
        if i >= size:
            break

        new_doc = doc.copy()
        new_id = str(uuid4())
        new_doc["task_id"] = new_id

        _start = _end + timedelta(minutes=i)
        _end = _start + timedelta(minutes=i + 1)

        new_doc["started_at"] = int(_start.timestamp())
        new_doc["ended_at"] = int(_end.timestamp())
        new_doc.pop("_id")
        new_docs.append(new_doc)
        new_task_ids.append(new_id)
        i += 1

    return new_docs, new_task_ids


class QueryTest(unittest.TestCase):
    URL = f"http://{WEBSERVER_HOST}:{WEBSERVER_PORT}{BASE_ROUTE}{TaskQuery.ROUTE}"

    @classmethod
    def setUpClass(cls):
        Thread(
            target=app.run,
            kwargs={"host": WEBSERVER_HOST, "port": WEBSERVER_PORT},
            daemon=True,
        ).start()
        sleep(2)

    def test_webserver_query(self):
        _filter = {"task_id": "1234"}
        request_data = {"filter": json.dumps(_filter)}

        r = requests.post(QueryTest.URL, json=request_data)
        assert r.status_code == 404

        docs, task_ids = gen_some_mock_data(size=1)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)

        _filter = {"task_id": task_ids[0]}
        request_data = {"filter": json.dumps(_filter)}
        r = requests.post(QueryTest.URL, json=request_data)
        assert r.status_code == 201
        assert docs[0]["task_id"] == r.json()[0]["task_id"]
        dao.delete_keys("task_id", docs[0]["task_id"])
        c1 = dao.count()
        assert c0 == c1

    def test_query_api(self):
        docs, task_ids = gen_some_mock_data(size=1)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)

        api = TaskQueryAPI(with_webserver=True)
        _filter = {"task_id": task_ids[0]}
        res = api.query(_filter)
        assert len(res) > 0
        assert docs[0]["task_id"] == res[0]["task_id"]
        dao.delete_keys("task_id", docs[0]["task_id"])
        c1 = dao.count()
        assert c0 == c1

    def test_query_without_webserver(self):
        docs, task_ids = gen_some_mock_data(size=1)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)

        api = TaskQueryAPI(with_webserver=False)
        _filter = {"task_id": task_ids[0]}
        res = api.query(_filter)
        assert len(res) > 0
        assert docs[0]["task_id"] == res[0]["task_id"]
        dao.delete_keys("task_id", docs[0]["task_id"])
        c1 = dao.count()
        assert c0 == c1

    def test_query_api_with_and_without_webserver(self):
        query_api_params = inspect.signature(TaskQueryAPI.query).parameters
        doc_query_api_params = inspect.signature(
            DocumentDBDao.task_query
        ).parameters
        assert (
            query_api_params == doc_query_api_params
        ), "Function signatures do not match."

        query_api_docstring = inspect.getdoc(TaskQueryAPI.query)
        doc_query_api_docstring = inspect.getdoc(DocumentDBDao.task_query)

        assert (
            query_api_docstring.strip() == doc_query_api_docstring.strip()
        ), "The docstrings are not equal."

        docs, task_ids = gen_some_mock_data(size=1)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)

        api_without = TaskQueryAPI(with_webserver=False)
        _filter = {"task_id": task_ids[0]}
        res_without = api_without.query(_filter)
        assert len(res_without) > 0
        assert docs[0]["task_id"] == res_without[0]["task_id"]

        api_with = TaskQueryAPI(with_webserver=True)
        res_with = api_with.query(_filter)
        assert len(res_with) > 0
        assert docs[0]["task_id"] == res_with[0]["task_id"]

        assert res_without == res_with

        dao.delete_keys("task_id", docs[0]["task_id"])
        c1 = dao.count()
        assert c0 == c1

    def test_aggregation(self):
        docs, task_ids = gen_some_mock_multi_workflow_data(size=100)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)
        sleep(3)
        api = TaskQueryAPI()
        res = api.query(
            aggregation=[
                ("max", "used.epochs"),
                ("max", "generated.accuracy"),
                ("avg", "used.batch_size"),
            ]
        )
        assert len(res) > 0
        for doc in res:
            if doc.get("max_generated_accuracy") is not None:
                assert doc["max_generated_accuracy"] > 0

        campaign_id = docs[0]["campaign_id"]
        res = api.query(
            filter={"campaign_id": campaign_id},
            aggregation=[
                ("max", "used.epochs"),
                ("max", "generated.accuracy"),
                ("avg", "used.batch_size"),
            ],
            sort=[
                ("max_used_epochs", TaskQueryAPI.ASC),
                ("ended_at", TaskQueryAPI.DESC),
            ],
            limit=10,
        )
        assert len(res) > 0
        for doc in res:
            if doc.get("max_generated_accuracy") is not None:
                assert doc["max_generated_accuracy"] > 0

        res = api.query(
            projection=["used.batch_size"],
            filter={"campaign_id": campaign_id},
            aggregation=[
                ("min", "generated.loss"),
                ("max", "generated.accuracy"),
            ],
            sort=[
                ("ended_at", TaskQueryAPI.DESC),
            ],
            limit=10,
        )
        assert len(res) > 1
        for doc in res:
            if doc.get("max_generated_accuracy") is not None:
                assert doc["max_generated_accuracy"] > 0

        dao.delete_keys("task_id", task_ids)
        c1 = dao.count()
        assert c0 == c1

    def test_query_df(self):
        max_docs = 5
        docs, task_ids = gen_some_mock_multi_workflow_data(size=max_docs)

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)
        sleep(1)
        api = TaskQueryAPI()

        _filter = {"task_id": {"$in": task_ids}}
        res = api.query_returning_df(
            _filter, remove_json_unserializables=False
        )
        assert len(res) == max_docs * 2
        dao.delete_keys("task_id", task_ids)
        c1 = dao.count()
        assert c0 == c1

    def test_query_df_telemetry(self):
        max_docs = 3
        docs, task_ids = gen_some_mock_data(
            size=max_docs, with_telemetry=True
        )

        dao = DocumentDBDao()
        c0 = dao.count()
        dao.insert_many(docs)
        sleep(1)
        api = TaskQueryAPI()

        _filter = {"task_id": {"$in": task_ids}}
        df = api.query_returning_df(
            _filter,
            remove_json_unserializables=False,
            calculate_telemetry_diff=True,
        )
        dao.delete_keys("task_id", task_ids)
        c1 = dao.count()
        assert c0 == c1

        assert len(df) == max_docs
        cleaned_df = clean_telemetry_dataframe(df)
        assert len(df.columns) > len(cleaned_df)
