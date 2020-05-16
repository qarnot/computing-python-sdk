import boto3
import botocore
import dateutil
import filecmp
import json
import os
import pytest
import requests
import requests_mock
import time

from boto3.s3.transfer import TransferConfig
from moto import mock_s3
from qarnot.bucket import Bucket
from qarnot.connection import Connection
from qarnot.exceptions import QarnotGenericException, MissingPoolException, MissingDiskException, MissingTaskException, MaxDiskException, MaxTaskException, NotEnoughCreditsException, UnauthorizedException,BucketStorageUnavailableException
from qarnot.pool import Pool
from qarnot.task import Task
from qarnot.storage import Storage
from shutil import rmtree
from unittest.mock import Mock, MagicMock, PropertyMock, call, patch

def create_pool():
    return '{' \
        + '"name": "name",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"creationDate": "2018-09-22T15:45:23.0Z",' \
        + '"uuid": "12345678-1234-1234-1234-123456789123",' \
        + '"state": "Success"' \
        + '}'

def create_task():
    return '{' \
        + '"name": "name",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"creationDate": "2018-09-22T15:45:23.0Z",' \
        + '"uuid": "12345678-1234-1234-1234-123456789123",' \
        + '"state": "Success"' \
        + '}'

def fill_s3_connection(connection):
    session = boto3.session.Session()
    client = session.client(service_name='s3',
                            aws_access_key_id="fake.fakey@McFakey.pants",
                            aws_secret_access_key="auth",
                            verify=True)
    resource = session.resource(service_name='s3',
                                aws_access_key_id="fake.fakey@McFakey.pants",
                                aws_secret_access_key="auth",
                                verify=True)

    with patch('qarnot.connection.Connection.s3client', new_callable=PropertyMock) as mock_client:
        with patch('qarnot.connection.Connection.s3resource', new_callable=PropertyMock) as mock_resource:
            with patch('qarnot.connection.Connection.user_info', new_callable=PropertyMock):
                mock_client.return_value = client
                mock_resource.return_value = resource
                connection._s3client = client
                connection._s3resource = resource
                return connection

@pytest.fixture(name="httpMock")
def create_http_mocker():
    with requests_mock.Mocker() as m:
        m.get("https://api.qarnot.com/settings",
            text='{"storage": "https://storage.qarnot.com"}')
        m.get("https://api.qarnot.com")
        m.get("https://api.qarnot.com/user",
            text='{"email": "fake.fakey@McFakey.pants",' \
            + '"diskCount": 0,' \
            + '"maxDisk": 200,' \
            + '"maxBucket": 0,' \
            + '"quotaBytesDisk": 203920,' \
            + '"quotaBytesBucket": 203920,' \
            + '"usedQuotaBytesDisk": 2000,' \
            + '"usedQuotaBytesBucket": 2000,' \
            + '"taskCount": 0,' \
            + '"maxTask": 200,' \
            + '"runningTaskCount": 0,' \
            + '"maxRunningTask": 10,' \
            + '"maxInstances": 20}')
        m.get("https://api.qarnot.com/info",
            text='{"email": "fake.fakey@McFakey.pants",' \
            + '"diskCount": 0,' \
            + '"maxDisk": 200,' \
            + '"maxBucket": 0,' \
            + '"quotaBytesDisk": 203920,' \
            + '"quotaBytesBucket": 203920,' \
            + '"usedQuotaBytesDisk": 2000,' \
            + '"usedQuotaBytesBucket": 2000,' \
            + '"taskCount": 0,' \
            + '"maxTask": 200,' \
            + '"runningTaskCount": 0,' \
            + '"maxRunningTask": 10,' \
            + '"maxInstances": 20}')
        yield m

                      
class TestConnection:
    @mock_s3
    def test_creation(self, httpMock):
        conn = Connection(client_token="token")

        assert conn.storage == "https://storage.qarnot.com"
        assert conn.cluster == "https://api.qarnot.com"
        assert conn.user_info.email == "fake.fakey@McFakey.pants"
        assert conn.user_info.disk_count == 0
        assert conn.user_info.max_disk == 200
        assert conn.user_info.max_bucket == 0
        assert conn.user_info.quota_bytes_disk == 203920
        assert conn.user_info.quota_bytes_bucket == 203920
        assert conn.user_info.used_quota_bytes_disk == 2000
        assert conn.user_info.used_quota_bytes_bucket == 2000
        assert conn.user_info.task_count == 0
        assert conn.user_info.max_task == 200
        assert conn.user_info.running_task_count == 0
        assert conn.user_info.max_running_task == 10
        assert conn.user_info.max_instances == 20

    @mock_s3
    def test_get_method(self, httpMock):
        httpMock.get("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=200)
        conn = Connection(client_token="token")
        response = conn._get("/message")
        assert response.status_code == 200 and response.json()["message"] == "Got it!"

    @mock_s3
    def test_get_method_unauthorized(self, httpMock):
        httpMock.get("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=401)
        conn = Connection(client_token="token")

        with pytest.raises(UnauthorizedException):
            response = conn._get("/message")
            assert response.status_code == 401 and response.json()["message"] == "Got it!"

    @mock_s3
    def test_patch_method_send_message(self, httpMock):
        httpMock.patch("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=200)
        conn = Connection(client_token="token")

        js = json.loads('{"message": "sent"}')
        response = conn._patch("/message", js)
        assert response.status_code == 200 and response.json()["message"] == "Got it!"
        assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_patch_method_unauthorized(self, httpMock):
        httpMock.patch("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=401)
        conn = Connection(client_token="token")

        with pytest.raises(UnauthorizedException):
            js = json.loads('{"message": "sent"}')
            response = conn._patch("/message", js)
            assert response.status_code == 401 and response.json()["message"] == "Got it!"
            assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_post_method_send_message(self, httpMock):
        httpMock.post("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=200)
        conn = Connection(client_token="token")

        js = json.loads('{"message": "sent"}')
        response = conn._post("/message", js)
        assert response.status_code == 200 and response.json()["message"] == "Got it!"
        assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_post_method_unauthorized(self, httpMock):
        httpMock.post("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=401)
        conn = Connection(client_token="token")

        with pytest.raises(UnauthorizedException):
            js = json.loads('{"message": "sent"}')
            response = conn._post("/message", js)
            assert response.status_code == 401 and response.json()["message"] == "Got it!"
            assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_delete_method(self, httpMock):
        httpMock.delete("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=200)
        conn = Connection(client_token="token")
        response = conn._delete("/message")
        assert response.status_code == 200 and response.json()["message"] == "Got it!"

    @mock_s3
    def test_delete_method_unauthorized(self, httpMock):
        httpMock.delete("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=401)
        conn = Connection(client_token="token")

        with pytest.raises(UnauthorizedException):
            response = conn._delete("/message")
            assert response.status_code == 401 and response.json()["message"] == "Got it!"

    @mock_s3
    def test_put_method_send_message(self, httpMock):
        httpMock.put("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=200)
        conn = Connection(client_token="token")

        js = json.loads('{"message": "sent"}')
        response = conn._put("/message", js)
        assert response.status_code == 200 and response.json()["message"] == "Got it!"
        assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_put_method_unauthorized(self, httpMock):
        httpMock.put("https://api.qarnot.com/message", text='{"message": "Got it!"}', status_code=401)
        conn = Connection(client_token="token")

        with pytest.raises(UnauthorizedException):
            js = json.loads('{"message": "sent"}')
            response = conn._put("/message", js)
            assert response.status_code == 401 and response.json()["message"] == "Got it!"
            assert httpMock.last_request.json()["message"] == "sent"

    @mock_s3
    def test_connection_user_info(self, httpMock):
        conn = Connection(client_token="token")
        user = conn.user_info

        assert httpMock.last_request.url == "https://api.qarnot.com/info"
        assert user.email == "fake.fakey@McFakey.pants"
        assert user.disk_count == 0
        assert user.max_disk == 200
        assert user.max_bucket == 0
        assert user.quota_bytes_disk == 203920
        assert user.quota_bytes_bucket == 203920
        assert user.used_quota_bytes_disk == 2000
        assert user.used_quota_bytes_bucket == 2000
        assert user.task_count == 0
        assert user.max_task == 200
        assert user.running_task_count == 0
        assert user.max_running_task == 10
        assert user.max_instances == 20

    @mock_s3
    def test_connection_get_buckets_with_no_s3_client(self, httpMock):
        conn = Connection(client_token="token")
        with pytest.raises(botocore.exceptions.ClientError):
            conn.buckets()

    @mock_s3
    def test_connection_get_buckets(self, httpMock):
        session = boto3.session.Session()
        client = session.client(service_name='s3',
                                aws_access_key_id="fake.fakey@McFakey.pants",
                                aws_secret_access_key="auth",
                                verify=True)
        resource = session.resource(service_name='s3',
                                    aws_access_key_id="fake.fakey@McFakey.pants",
                                    aws_secret_access_key="auth",
                                    verify=True)
        client.create_bucket(Bucket="bucket1")
        client.create_bucket(Bucket="bucket2")

        with patch('qarnot.connection.Connection.s3client', new_callable=PropertyMock) as mock_client:
            with patch('qarnot.connection.Connection.s3resource', new_callable=PropertyMock) as mock_resource:
                with patch('qarnot.connection.Connection.user_info', new_callable=PropertyMock):
                    mock_client.return_value = client
                    mock_resource.return_value = resource
                    conn = Connection(client_token="token")
                    conn._s3client = client
                    conn._s3resource = resource
                    buckets = list(map(lambda x: x._uuid, conn.buckets()))

                    assert "bucket1" in buckets and "bucket2" in buckets
    
    @mock_s3
    def test_connection_get_pools(self, httpMock):
        httpMock.get("https://api.qarnot.com/pools", text="{}")
        conn = Connection(client_token="token")
        conn.pools(False)

        assert httpMock.last_request.url == "https://api.qarnot.com/pools"

    @mock_s3
    def test_connection_get_pool_summaries(self, httpMock):
        httpMock.get("https://api.qarnot.com/pools/summaries", text="{}")
        conn = Connection(client_token="token")
        conn.pools()

        assert httpMock.last_request.url == "https://api.qarnot.com/pools/summaries"

    @mock_s3
    def test_connection_get_tasks(self, httpMock):
        httpMock.get("https://api.qarnot.com/tasks", text="{}")
        conn = Connection(client_token="token")
        conn.tasks(summary=False)

        assert httpMock.last_request.url == "https://api.qarnot.com/tasks"

    @mock_s3
    def test_connection_get_tasks_summaries(self, httpMock):
        httpMock.get("https://api.qarnot.com/tasks/summaries", text="{}")
        conn = Connection(client_token="token")
        conn.tasks(summary=True)

        assert httpMock.last_request.url == "https://api.qarnot.com/tasks/summaries"

    @mock_s3
    def test_connection_retrieve_pool(self, httpMock):
        httpMock.get(
            "https://api.qarnot.com/pools/12345678-1234-1234-1234-123456789123",
            text=create_pool())
        conn = Connection(client_token="token")
        with patch.object(Pool, 'from_json') as pool_from_json:
            conn.retrieve_pool("12345678-1234-1234-1234-123456789123")

            assert httpMock.last_request.url == "https://api.qarnot.com/pools/12345678-1234-1234-1234-123456789123"
            pool_from_json.assert_called_with(conn, json.loads(create_pool()))

    @mock_s3
    def test_connection_retrieve_pool_404(self, httpMock):
        httpMock.get(
            "https://api.qarnot.com/pools/12345678-1234-1234-1234-123456789123",
            text='{"message":"missing pool"}',
            status_code=404)
        conn = Connection(client_token="token")
        with pytest.raises(MissingPoolException):
            conn.retrieve_pool("12345678-1234-1234-1234-123456789123")

    @mock_s3
    def test_connection_retrieve_task(self, httpMock):
        httpMock.get(
            "https://api.qarnot.com/tasks/12345678-1234-1234-1234-123456789123",
            text=create_task())
        conn = Connection(client_token="token")

        with patch.object(Task, 'from_json') as task_from_json:
            conn.retrieve_task("12345678-1234-1234-1234-123456789123")
            assert httpMock.last_request.url == "https://api.qarnot.com/tasks/12345678-1234-1234-1234-123456789123"
            task_from_json.assert_called_with(conn, json.loads(create_task()))

    @mock_s3
    def test_connection_retrieve_task_404(self, httpMock):
        httpMock.get(
            "https://api.qarnot.com/tasks/12345678-1234-1234-1234-123456789123",
            text='{"message": "missing task"}',
            status_code=404)
        conn = Connection(client_token="token")
        with pytest.raises(MissingTaskException):
            conn.retrieve_task("12345678-1234-1234-1234-123456789123")

    @mock_s3
    def test_create_bucket(self, httpMock):
        conn = fill_s3_connection(Connection(client_token="token"))

        conn.retrieve_or_create_bucket("bucket")
        response = conn.s3client.list_buckets()

        assert "bucket" in list(map(lambda x: x["Name"], response["Buckets"]))

    @mock_s3
    def test_retrieve_or_create_bucket_retrieve_mode(self, httpMock):
        conn = fill_s3_connection(Connection(client_token="token"))

        conn.s3client.create_bucket(Bucket="bucket")

        bucket = conn.retrieve_or_create_bucket("bucket")
        response = conn.s3client.list_buckets()

        assert list(response["Buckets"]) and "bucket" in list(map(lambda x: x["Name"], response["Buckets"]))
        assert bucket.uuid == "bucket"
    
    @mock_s3
    def test_retrieve_bucket_bucket_exists(self, httpMock):
        conn = fill_s3_connection(Connection(client_token="token"))

        conn.s3client.create_bucket(Bucket="bucket")
        bucket = conn.retrieve_bucket("bucket")
        response = conn.s3client.list_buckets()

        assert "bucket" in list(map(lambda x: x["Name"], response["Buckets"]))
        assert bucket.uuid == "bucket"

    @mock_s3
    def test_retrieve_bucket_buckets_does_not_exist(self, httpMock):
        conn = fill_s3_connection(Connection(client_token="token"))

        with pytest.raises(botocore.exceptions.ClientError):
            conn.retrieve_bucket("bucket")

    @mock_s3
    def test_create_pool(self, httpMock):
        conn = Connection(client_token="token")
        pool = conn.create_pool("pool", "docker-batch", 1, "shortpool")

        assert pool.name == "pool" and pool.profile == "docker-batch" and pool.instancecount == 1 and pool.shortname == "shortpool"


    @mock_s3
    def test_create_task(self, httpMock):
        """
        This test Fails!!
        Task is created without shortname for reason unknown even after investigation
        """

        conn = Connection(client_token="token")
        task = conn.create_task("task", "docker-batch", 1, shortname="shorttask")

        assert task.name == "task" and task.profile == "docker-batch" and task.instancecount == 1 and task.shortname == "shorttask"

    @mock_s3
    def test_submit_bulk_task(self, httpMock):
        httpMock.post("https://api.qarnot.com/tasks",
                        text= '['
                            + '{"uuid":"12345678-1234-1234-1234-123456789123", "statusCode": 200, "message": "OK"},'
                            + '{"uuid":"12345678-1234-1234-1234-234567891234", "statusCode": 200, "message": "OK"}'
                            + ']')
        conn = Connection(client_token="token")
        tasks = [Task(conn,
                    "task1",
                    "docker-batch",
                    1,
                    "shortname"),
                Task(conn,
                    "task2",
                    "docker-batch",
                    1)]
        with patch.object(Task, 'update'):
            conn.submit_tasks(tasks)
            assert tasks[0].uuid == "12345678-1234-1234-1234-123456789123"
            assert tasks[1].uuid == "12345678-1234-1234-1234-234567891234"

    @mock_s3
    def test_submit_service_unavailable(self, httpMock):
        httpMock.post("https://api.qarnot.com/tasks",
                        text= '['
                            + '{"uuid":"12345678-1234-1234-1234-123456789123", "statusCode": 200, "message": "OK"},'
                            + '{"uuid":"12345678-1234-1234-1234-234567891234", "statusCode": 200, "message": "OK"}'
                            + ']',
                        status_code=503)
        conn = Connection(client_token="token")
        tasks = [Task(conn,
                    "task1",
                    "docker-batch",
                    1,
                    "shortname"),
                Task(conn,
                    "task2",
                    "docker-batch",
                    1)]
        with pytest.raises(QarnotGenericException):
            with patch.object(Task, 'update'):
                conn.submit_tasks(tasks)
                assert tasks[0].uuid == "12345678-1234-1234-1234-123456789123"
                assert tasks[1].uuid == "12345678-1234-1234-1234-234567891234"

    @mock_s3
    def test_get_profiles(self, httpMock):
        httpMock.get("https://api.qarnot.com/profiles",
                     text='["docker-batch", "docker-network", "blender"]')
        httpMock.get("https://api.qarnot.com/profiles/docker-batch",
                     text='{"name": "docker-batch", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')
        httpMock.get("https://api.qarnot.com/profiles/docker-network",
                     text='{"name": "docker-network", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')
        httpMock.get("https://api.qarnot.com/profiles/blender",
                     text='{"name": "blender", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')

        conn = Connection(client_token="token")

        l = conn.profiles()
        assert len(l) == 3
        assert l[0].name == "docker-batch" and l[0].constants[0][0] == "DOCKER_CMD" and l[0].constants[0][1] == "nope"
        assert l[1].name == "docker-network" and l[1].constants[0][0] == "DOCKER_CMD" and l[1].constants[0][1] == "nope"
        assert l[2].name == "blender" and l[2].constants[0][0] == "DOCKER_CMD" and l[2].constants[0][1] == "nope"

    @mock_s3
    def test_get_profiles_partialy_missing(self, httpMock):
        httpMock.get("https://api.qarnot.com/profiles",
                     text='["docker-batch", "docker-network", "blender"]')
        httpMock.get("https://api.qarnot.com/profiles/docker-batch",
                     text='{"name": "docker-batch", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')
        httpMock.get("https://api.qarnot.com/profiles/docker-network",
                     text='{"message": "unknown profile"}',
                     status_code=404)
        httpMock.get("https://api.qarnot.com/profiles/blender",
                     text='{"name": "blender", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')

        conn = Connection(client_token="token")

        l = conn.profiles()
        assert len(l) == 2
        assert l[0].name == "docker-batch" and l[0].constants[0][0] == "DOCKER_CMD" and l[0].constants[0][1] == "nope"
        assert l[1].name == "blender" and l[1].constants[0][0] == "DOCKER_CMD" and l[1].constants[0][1] == "nope"
    
    @mock_s3
    def test_retrieve_profile(self, httpMock):
        httpMock.get("https://api.qarnot.com/profiles/docker-batch",
                     text='{"name": "docker-batch", "constants": [{"name": "DOCKER_CMD", "value": "nope"}]}')

        conn = Connection(client_token="token")

        prof = conn.retrieve_profile("docker-batch")
        assert prof.name == "docker-batch" and prof.constants[0][0] == "DOCKER_CMD" and prof.constants[0][1] == "nope"

    @mock_s3
    def test_retrieve_inexistant_profile(self, httpMock):
        httpMock.get("https://api.qarnot.com/profiles/docker-batch",
                     text='{"message": "missing"}',
                     status_code=404)

        conn = Connection(client_token="token")

        with pytest.raises(QarnotGenericException):
            conn.retrieve_profile("docker-batch")

    @mock_s3
    def test_connection_create_bucket(self, httpMock):
        conn = fill_s3_connection(Connection(client_token="token"))

        bucket = conn.create_bucket("bucket")

        response = conn.s3client.list_buckets()

        assert list(response["Buckets"]) and "bucket" in list(map(lambda x: x["Name"], response["Buckets"]))
        assert bucket.uuid == "bucket"