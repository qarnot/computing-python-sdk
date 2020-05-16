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
from qarnot.exceptions import MissingDiskException, MissingTaskException, MaxDiskException, MaxTaskException, NotEnoughCreditsException
from qarnot.pool import Pool
from qarnot.task import Task
from qarnot.storage import Storage
from shutil import rmtree
from unittest.mock import Mock, MagicMock, PropertyMock, call, patch


@pytest.fixture(name='connection')
def qarnot_connection():
    with requests_mock.Mocker() as m:
        m.get("https://api.qarnot.com/settings",
              text='{"storage": "https://storage.qarnot.com"}')
        m.get("https://api.qarnot.com")
        m.get("https://api.qarnot.com/user",
              text='{"email": "fake.fakey@McFakey.pants"}')
        m.get("https://api.qarnot.com/info",
              text='{"email": "fake.fakey@McFakey.pants"}')
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
                    conn = Connection(client_token="token")
                    conn._s3client = client
                    conn._s3resource = resource
                    yield conn


class TestBucket:
    AWS_UPLOAD_MAX_SIZE = 8 * 1024 * 1024
    # Size of parts when uploading in parts
    AWS_UPLOAD_PART_SIZE = 8 * 1024 * 1024

    s3_multipart_config = TransferConfig(
        multipart_threshold=AWS_UPLOAD_MAX_SIZE,
        multipart_chunksize=AWS_UPLOAD_PART_SIZE,
        max_concurrency=10,
        num_download_attempts=10,
    )

    @mock_s3
    def test_create_bucket(self, connection):
        bucket = Bucket(connection, "bucket")

        response = connection.s3client.list_buckets()

        assert bucket._uuid == "bucket" and len(
            response["Buckets"]) == 1 and response["Buckets"][0]['Name'] == "bucket"

    @mock_s3
    def test_create_local_representation_of_bucket(self, connection):
        bucket = Bucket(connection, "bucket", False)

        response = connection.s3client.list_buckets()

        assert bucket._uuid == "bucket" and len(response["Buckets"]) == 0

    @mock_s3
    def test_create_already_existing_bucket(self, connection):
        bucket1 = Bucket(connection, "bucket")
        bucket2 = Bucket(connection, "bucket")

        response = connection.s3client.list_buckets()

        assert bucket1._uuid == "bucket" and bucket2._uuid == "bucket" \
            and len(response["Buckets"]) == 1 and response["Buckets"][0]["Name"] == "bucket"

    @mock_s3
    def test_retrieve_bucket_from_uuid(self, connection):
        connection.s3client.create_bucket(Bucket='retrievable')

        bucket = Bucket._retrieve(connection, "retrievable")

        assert bucket._uuid == "retrievable" and bucket._connection == connection

    @mock_s3
    def test_delete_empty_bucket(self, connection):
        bucket = Bucket(connection, "bucket")

        bucket.delete()

        response = connection.s3client.list_buckets()

        assert len(response["Buckets"]) == 0

    @mock_s3
    def test_list_files_from_empty_directory(self, connection):
        bucket = Bucket(connection, "bucket")

        files = bucket.list_files()

        count = 0
        for _ in files:
            count += 1

        assert count == 0

    @mock_s3
    def test_add_file_to_bucket(self, connection):
        bucket = Bucket(connection, "bucket")

        bucket.add_file("test/Unit_Test/Assets/lorem.txt")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        bucket = ""
        for f in files:
            names.append(f.key)
            bucket = f.bucket_name

        assert len(
            names) == 1 and names[0] == "lorem.txt" and bucket == "bucket"

    @mock_s3
    def test_add_file_to_bucket_with_specified_name(self, connection):
        bucket = Bucket(connection, "bucket")
        bucket.add_file("test/Unit_Test/Assets/lorem.txt", "loremo.te")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        assert len(
            names) == 1 and names[0][0] == "loremo.te" and names[0][1] == "bucket"

    @mock_s3
    def test_add_file_to_bucket_with_empty_name(self, connection):
        bucket = Bucket(connection, "bucket")
        bucket.add_file("test/Unit_Test/Assets/lorem.txt", "")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        assert len(
            names) == 1 and names[0][0] == "lorem.txt" and names[0][1] == "bucket"

    @mock_s3
    def test_add_multiple_file_to_bucket_with_varied_names(self, connection):
        bucket = Bucket(connection, "bucket")
        bucket.add_file("test/Unit_Test/Assets/lorem.txt")
        bucket.add_file("test/Unit_Test/Assets/lorem.txt", "variedname")
        bucket.add_file("test/Unit_Test/Assets/lorem.txt",
                        "varied/dir/variedname")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "variedname", names))[0]
        file3 = list(
            filter(lambda x: x[0] == "varied/dir/variedname", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "bucket"
        assert file2 is not None and file2[1] == "bucket"
        assert file3 is not None and file3[1] == "bucket"

    @mock_s3
    def test_add_multiple_file_to_multiple_bucket_with_varied_names(self, connection):
        bucket1 = Bucket(connection, "bucket1")
        bucket2 = Bucket(connection, "bucket2")

        bucket1.add_file("test/Unit_Test/Assets/lorem.txt")
        bucket2.add_file("test/Unit_Test/Assets/lorem.txt", "variedname")
        bucket1.add_file("test/Unit_Test/Assets/lorem.txt",
                         "varied/dir/variedname")

        files1 = connection.s3resource.Bucket(bucket1._uuid).objects.all()
        files2 = connection.s3resource.Bucket(bucket2._uuid).objects.all()

        names = []
        for f in files1:
            names.append((f.key, f.bucket_name))
        for f in files2:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "variedname", names))[0]
        file3 = list(
            filter(lambda x: x[0] == "varied/dir/variedname", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "bucket1"
        assert file2 is not None and file2[1] == "bucket2"
        assert file3 is not None and file3[1] == "bucket1"

    @mock_s3
    def test_add_directory(self, connection):
        bucket = Bucket(connection, "bucket")
        bucket.add_directory("test/Unit_Test/Assets/")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "fichier_important.asm", names))[0]
        file3 = list(filter(lambda x: x[0] == "nested/file", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "bucket"
        assert file2 is not None and file2[1] == "bucket"
        assert file3 is not None and file3[1] == "bucket"

    @mock_s3
    def test_add_directory_with_specified_remote_name(self, connection):
        bucket = Bucket(connection, "bucket")
        bucket.add_directory("test/Unit_Test/Assets/", "brouette")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "brouette/lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "brouette/fichier_important.asm", names))[0]
        file3 = list(filter(lambda x: x[0] == "brouette/nested/file", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "bucket"
        assert file2 is not None and file2[1] == "bucket"
        assert file3 is not None and file3[1] == "bucket"

    @mock_s3
    def test_add_non_existant_directory(self, connection):
        bucket = Bucket(connection, "bucket")
        with pytest.raises(IOError):
            bucket.add_directory("test/Unit_Test/Assets/nope")

    @mock_s3
    def test_add_empty_directory(self, connection):
        os.mkdir("test/Unit_Test/Assets/empty")
        bucket = Bucket(connection, "bucket")
        bucket.add_directory("test/Unit_Test/Assets/empty")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        os.rmdir("test/Unit_Test/Assets/empty")

        assert len(names) == 0

    @mock_s3
    def test_get_all_files_from_bucket_files_different (self, connection):
        os.mkdir("test/Unit_Test/putput")
        bucket = Bucket(connection, "bucket")
        with patch.object(bucket, '_download_file', wraps=bucket._download_file) as wrapped_df:
            with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
            with open("test/Unit_Test/Assets/fichier_important.asm", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
            with open("test/Unit_Test/Assets/nested/file", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

            files = connection.s3resource.Bucket(bucket._uuid).objects.all()

            names = []
            for f in files:
                names.append((f.key, f.bucket_name))

            file1_same = file2_same = file3_same = False
            try:
                bucket.get_all_files("test/Unit_Test/putput")
                print("no problem")
                file1_same = filecmp.cmp("test/Unit_Test/putput/lorem.txt", "test/Unit_Test/Assets/lorem.txt")
                file2_same = filecmp.cmp("test/Unit_Test/putput/variedname", "test/Unit_Test/Assets/fichier_important.asm")
                file3_same = filecmp.cmp("test/Unit_Test/putput/varied/dir/variedname", "test/Unit_Test/Assets/nested/file")
                rmtree("test/Unit_Test/putput")
            except Exception as e:
                print(e)
                rmtree("test/Unit_Test/putput")

            assert wrapped_df.call_count == 3
            assert file1_same and file2_same and file3_same

    @mock_s3
    def test_get_all_files_from_bucket_identical_content(self, connection):
        os.mkdir("test/Unit_Test/putput")
        bucket = Bucket(connection, "bucket")
        with patch.object(bucket, '_download_file', wraps=bucket._download_file) as wrapped_df:
            with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
            with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
            with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
                connection.s3client.upload_fileobj(
                    f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

            files = connection.s3resource.Bucket(bucket._uuid).objects.all()

            names = []
            for f in files:
                names.append((f.key, f.bucket_name))

            file1_same = file2_same = file3_same = False
            try:
                bucket.get_all_files("test/Unit_Test/putput")
                file1_same = filecmp.cmp("test/Unit_Test/putput/lorem.txt", "test/Unit_Test/Assets/lorem.txt")
                file2_same = filecmp.cmp("test/Unit_Test/putput/variedname", "test/Unit_Test/Assets/lorem.txt")
                file3_same = filecmp.cmp("test/Unit_Test/putput/varied/dir/variedname", "test/Unit_Test/Assets/lorem.txt")
                rmtree("test/Unit_Test/putput")
            except:
                rmtree("test/Unit_Test/putput")

            assert wrapped_df.call_count == 1
            assert file1_same and file2_same and file3_same

    @mock_s3
    def test_get_file(self, connection):
        bucket = Bucket(connection, "bucket")
        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)

        bucket._download_file = MagicMock()

        bucket.get_file("lorem.txt", local="LOcalREMote.txt")

        bucket._download_file.assert_any_call(
            "lorem.txt", "LOcalREMote.txt", None)

    @mock_s3
    def test_get_non_existent_file(self, connection):
        bucket = Bucket(connection, "bucket")

        with pytest.raises(botocore.exceptions.ClientError):
            bucket.get_file("existn't")

    @mock_s3
    def test_copy_file(self, connection):
        bucket = Bucket(connection, "bucket")

        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)

        bucket.copy_file("lorem.txt", "lorem2-electric-boogaloo.txt")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        f = list(filter(lambda x: x[0] == "lorem2-electric-boogaloo.txt", names))[0]

        os.mkdir("compare")

        bucket._download_file("lorem.txt", "compare/lorem.txt")
        bucket._download_file("lorem2-electric-boogaloo.txt", "compare/lorem2-electric-boogaloo.txt")

        compare = filecmp.cmp("compare/lorem.txt", "compare/lorem2-electric-boogaloo.txt")
        rmtree("compare")

        assert len(names) == 2
        assert f is not None and f[1] == "bucket"
        assert compare

    @mock_s3
    def test_copy_non_existent_file(self, connection):
        bucket = Bucket(connection, "bucket")

        with pytest.raises(botocore.exceptions.ClientError):
            bucket.copy_file("lorem.txt", "lorem2-electric-boogaloo.txt")

    @mock_s3
    def test_list_files_from_bucket(self, connection):
        bucket = Bucket(connection, "bucket")

        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/fichier_important.asm", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/nested/file", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

        files = bucket.list_files()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "variedname", names))[0]
        file3 = list(filter(lambda x: x[0] == "varied/dir/variedname", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "bucket"
        assert file2 is not None and file2[1] == "bucket"
        assert file3 is not None and file3[1] == "bucket"

    @mock_s3
    def test_list_files_from_directory(self, connection):
        bucket = Bucket(connection, "bucket")

        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/fichier_important.asm", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/nested/file", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

        files = bucket.directory("varied")

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))
        file2 = list(filter(lambda x: x[0] == "variedname", names))
        file3 = list(filter(lambda x: x[0] == "varied/dir/variedname", names))

        assert len(names) == 2
        assert file1 == []
        assert file2 != [] and file2[0][1] == "bucket"
        assert file3 != [] and file3[0][1] == "bucket"

    @mock_s3
    def test_delete_a_file_from_bucket(self, connection):
        bucket = Bucket(connection, "bucket")
        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/fichier_important.asm", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/nested/file", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

        bucket.delete_file("variedname")

        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))
        file2 = list(filter(lambda x: x[0] == "variedname", names))
        file3 = list(
            filter(lambda x: x[0] == "varied/dir/variedname", names))

        assert len(names) == 2
        assert file1 != [] and file1[0][1] == "bucket"
        assert file2 == []
        assert file3 != [] and file3[0][1] == "bucket"

    @mock_s3
    def test_delete_non_empty_bucket(self, connection):
        bucket = Bucket(connection, "bucket")
        with open("test/Unit_Test/Assets/lorem.txt", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "lorem.txt", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/fichier_important.asm", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "variedname", Config=self.s3_multipart_config)
        with open("test/Unit_Test/Assets/nested/file", "rb") as f:
            connection.s3client.upload_fileobj(
                f, bucket._uuid, "varied/dir/variedname", Config=self.s3_multipart_config)

        bucket.delete()

        response = connection.s3client.list_buckets()

        assert len(response["Buckets"]) == 0

    @mock_s3
    def test_sync_asset_directory(self, connection):
        bucket = Bucket(connection, "syncbucket")
        
        bucket.sync_directory("test/Unit_Test/Assets")
        files = connection.s3resource.Bucket(bucket._uuid).objects.all()

        names = []
        for f in files:
            names.append((f.key, f.bucket_name))

        file1 = list(filter(lambda x: x[0] == "lorem.txt", names))[0]
        file2 = list(filter(lambda x: x[0] == "fichier_important.asm", names))[0]
        file3 = list(filter(lambda x: x[0] == "nested/file", names))[0]

        assert len(names) == 3
        assert file1 is not None and file1[1] == "syncbucket"
        assert file2 is not None and file2[1] == "syncbucket"
        assert file3 is not None and file3[1] == "syncbucket"