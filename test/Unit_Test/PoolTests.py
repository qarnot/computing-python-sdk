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
from qarnot.exceptions import QarnotGenericException, MaxPoolException, MissingPoolException, MissingDiskException, MissingTaskException, MaxDiskException, MaxTaskException, NotEnoughCreditsException, UnauthorizedException, BucketStorageUnavailableException
from qarnot.pool import Pool
from qarnot.task import Task
from qarnot.storage import Storage
from shutil import rmtree
from unittest.mock import Mock, MagicMock, PropertyMock, call, patch

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

class TestPool:
    def test_create_pool(self, httpMock):
        conn = Mock(Connection)
        pool = Pool(conn, "pool", "docker-batch", 1, shortname= "shortpool")

        assert pool.name == "pool"
        assert pool.profile == "docker-batch"
        assert pool.state == "UnSubmitted"
        assert pool._connection == conn
        assert pool.instancecount == 1
        assert pool.uuid == None

    def test_retrieve_pool_from_uuid(self, httpMock):
        this = httpMock.get("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000", text='{"message": "done"}')
        conn = Connection(client_token="token")
        with patch.object(Pool, 'from_json') as from_json:
            Pool._retrieve(conn, "00000000-0000-0000-0000-000000000000")

            assert this.called
            from_json.assert_called_with(conn, json.loads('{"message":"done"}'))

    def test_retrieve_unexising_pool(self, httpMock):
        this = httpMock.get("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000",
                            text='{"message": "problem"}',
                            status_code=404)

        conn = Connection(client_token="token")
        with pytest.raises(MissingPoolException):
            with patch.object(Pool, 'from_json') as from_json:
                Pool._retrieve(conn, "00000000-0000-0000-0000-000000000000")

                assert this.called
                from_json.assert_called_with(conn, json.loads('{"message":"problem"}'))

    def test_create_new_pool_from_json(self, httpMock):
        js = json.loads('{' \
        + '"name": "pool",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"shortname": "shortpool",' \
        + '"creationDate": "2019-09-08T12:46:21Z",' \
        + '"uuid": "00000000-0000-0000-0000-000000000000",' \
        + '"state": "Submitted"' \
        + '}')

        conn = Mock(Connection)
        pool = Pool.from_json(conn, js)
        assert pool.name == "pool"
        assert pool.profile == "docker-batch"
        assert pool.instancecount == 1
        assert pool.shortname == "shortpool"
        assert pool.creation_date == dateutil.parser.parse("2019-09-08 12:46:21")
        assert pool.uuid == "00000000-0000-0000-0000-000000000000"
        assert pool.state == "Submitted"

    def test_convert_pool_to_json(self, httpMock):
        js = json.loads('{' \
        + '"name": "pool",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"shortname": "shortpool",' \
        + '"constants": [],' \
        + '"constraints": [],' \
        + '"resourceBuckets": [],' \
        + '"resourceDisks": [],' \
        + '"tags": []' \
        + '}')

        conn = Connection(client_token='token')
        pool = Pool(conn, "pool", "docker-batch", 1, "shortpool")

        pooljs = pool._to_json()

        assert js == pooljs
        
    def test_submit_pool(self, httpMock):
        with patch.object(Pool, 'update'):
            this = httpMock.post("https://api.qarnot.com/pools", text='{"uuid":"00000000-0000-0000-0000-000000000000"}')
            conn = Connection(client_token="token")
            pool = Pool(conn, "pool", "docker-batch", 1)
            pool.submit()

            assert this.called
            assert pool.uuid == "00000000-0000-0000-0000-000000000000"

    def test_submit_non_existing_pool(self, httpMock):
        with patch.object(Pool, 'update'):
            this = httpMock.post("https://api.qarnot.com/pools",
                                text='{"message":"problem"}',
                                status_code=404)
            conn = Connection(client_token="token")
            pool = Pool(conn, "pool", "docker-batch", 1)
            with pytest.raises(MissingDiskException):
                pool.submit()

                assert this.called

    def test_submit_pool_but_no_disks_available(self, httpMock):
        with patch.object(Pool, 'update'):
            this = httpMock.post("https://api.qarnot.com/pools",
                                text='{"message":"Maximum number of disks reached"}',
                                status_code=403)
            conn = Connection(client_token="token")
            pool = Pool(conn, "pool", "docker-batch", 1)
            with pytest.raises(MaxDiskException):
                pool.submit()

                assert this.called

    def test_submit_pool_but_no_buckets_available(self, httpMock):
        with patch.object(Pool, 'update'):
            this = httpMock.post("https://api.qarnot.com/pools",
                                text='{"message":"Maximum number of buckets reached"}',
                                status_code=403)
            conn = Connection(client_token="token")
            pool = Pool(conn, "pool", "docker-batch", 1)
            with pytest.raises(MaxPoolException):
                pool.submit()

                assert this.called

    def test_submit_pool_but_no_credits(self, httpMock):
        with patch.object(Pool, 'update'):
            this = httpMock.post("https://api.qarnot.com/pools",
                                text='{"message":"Not enough credits"}',
                                status_code=402)
            conn = Connection(client_token="token")
            pool = Pool(conn, "pool", "docker-batch", 1)
            with pytest.raises(NotEnoughCreditsException):
                pool.submit()

                assert this.called

    def test_update_existing_pool(self, httpMock):
        jstring = '{' \
        + '"name": "pool",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"shortname": "shortpool",' \
        + '"constants": [],' \
        + '"constraints": [],' \
        + '"resourceBuckets": [],' \
        + '"resourceDisks": [],' \
        + '"tags": [],' \
        + '"creationDate": "2019-09-08T12:46:21Z",' \
        + '"uuid": "00000000-0000-0000-0000-000000000000",' \
        + '"state": "Submitted"' \
        + '}'

        this = httpMock.get('https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000',
                            text=jstring)
        conn = Connection(client_token="token")
        pool = Pool(conn, "pool", "docker-batch", 1)
        pool._uuid = "00000000-0000-0000-0000-000000000000"

        pool.update(True)

        assert this.called
        assert pool.state == 'Submitted'
        assert pool.uuid == '00000000-0000-0000-0000-000000000000'

    def test_update_non_existing_pool(self, httpMock):
        this = httpMock.get('https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000',
                            text='{"message":"missing pool"}',
                            status_code=404)

        conn = Connection(client_token="token")
        pool = Pool(conn, "pool", "docker-batch", 1)
        pool._uuid = "00000000-0000-0000-0000-000000000000"

        with pytest.raises(MissingPoolException):
            pool.update(True)

            assert this.called
            assert pool.state == 'Unsubmitted'
            assert pool.uuid == None

    def test_delete(self, httpMock):
        this = httpMock.delete("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000")
        
        conn = Connection(client_token="token")
        pool = Pool(conn, "pool", "docker-batch", 1)
        pool._uuid = "00000000-0000-0000-0000-000000000000"
        
        pool.delete(False)

        assert this.called
        assert pool.uuid == None
        assert pool.state == "Deleted"

    @mock_s3
    def test_delete_do_not_purge_resources(self, httpMock):
        this = httpMock.delete("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000")
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
                    bucket = Bucket(conn, "bucket")
                    pool = Pool(conn, "pool", "docker-batch", 1)
                    pool._uuid = "00000000-0000-0000-0000-000000000000"

                    pool.resources.append(bucket)
                    
                    pool.delete(False)

                    assert this.called
                    assert pool.uuid == None
                    assert pool.state == "Deleted"
                    assert pool.resources == [bucket]

    @mock_s3
    def test_delete_purge_resources(self, httpMock):
        this = httpMock.delete("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000")
        session = boto3.session.Session()
        client = session.client(service_name='s3',
                                aws_access_key_id="fake.fakey@McFakey.pants",
                                aws_secret_access_key="auth",
                                verify=True)
        with patch('qarnot.connection.Connection.s3client', new_callable=PropertyMock) as mock_client:
            with patch('qarnot.connection.Connection.user_info', new_callable=PropertyMock):
                mock_client.return_value = client
                conn = Connection(client_token="token")
                bucket = Bucket(conn, "bucket")
                pool = Pool(conn, "pool", "docker-batch", 1)
                pool._uuid = "00000000-0000-0000-0000-000000000000"

                pool.resources.append(bucket)

                response = client.list_buckets()
                assert len(response["Buckets"]) == 1
                
                pool.delete(True)

                response = client.list_buckets()

                assert this.called
                assert pool.uuid == None
                assert pool.state == "Deleted"
                assert pool.resources == [bucket]
                assert len(response["Buckets"]) == 0
    
    def test_close_existing_pool(self, httpMock):
        jstring_after = '{' \
        + '"name": "pool",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"shortname": "shortpool",' \
        + '"constants": [],' \
        + '"constraints": [],' \
        + '"resourceBuckets": [],' \
        + '"resourceDisks": [],' \
        + '"tags": [],' \
        + '"creationDate": "2019-09-08T12:46:21Z",' \
        + '"uuid": "00000000-0000-0000-0000-000000000000",' \
        + '"state": "Closed"' \
        + '}'

        post = httpMock.post("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000/close")
        get = httpMock.get('https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000',
                            text=jstring_after)
        conn = Connection(client_token="token")
        pool = Pool(conn, "pool", "docker-batch", 1)
        pool._uuid = "00000000-0000-0000-0000-000000000000"

        pool.close()

        assert post.called
        assert get.call_count == 2
        assert get.called
        assert pool.state == 'Closed'

    def test_close_non_existing_pool(self, httpMock):
        jstring_after = '{' \
        + '"name": "pool",' \
        + '"profile": "docker-batch",' \
        + '"instanceCount": 1,' \
        + '"shortname": "shortpool",' \
        + '"constants": [],' \
        + '"constraints": [],' \
        + '"resourceBuckets": [],' \
        + '"resourceDisks": [],' \
        + '"tags": [],' \
        + '"creationDate": "2019-09-08t12:46:21z",' \
        + '"uuid": "00000000-0000-0000-0000-000000000000",' \
        + '"state": "closed"' \
        + '}'

        post = httpMock.post("https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000/close",
                             text='{"message":"problem"}',
                             status_code=404)
        get = httpMock.get('https://api.qarnot.com/pools/00000000-0000-0000-0000-000000000000',
                            text=jstring_after)
        conn = Connection(client_token="token")
        pool = Pool(conn, "pool", "docker-batch", 1)
        pool._uuid = "00000000-0000-0000-0000-000000000000"

        with pytest.raises(MissingPoolException):
            pool.close()

            assert post.called
            assert get.call_count == 1
            assert get.called
