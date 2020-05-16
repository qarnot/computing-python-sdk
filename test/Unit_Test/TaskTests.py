import boto3
import botocore
import dateutil
import json
import pytest
import requests
import time

from qarnot.bucket import Bucket
from qarnot.connection import Connection
from qarnot.exceptions import MissingDiskException, MissingTaskException, MaxDiskException, MaxTaskException, NotEnoughCreditsException
from qarnot.pool import Pool
from qarnot.task import Task
from qarnot.bucket import Bucket
from unittest.mock import Mock, MagicMock, call

class TestTask:
    def test_create_task(self):
        task = Task(None, "name", "docker-batch", 1)
        assert task.name == "name" and task.profile == "docker-batch" and task.instancecount == 1
    
    def test_create_task_with_pool(self):
        mock = Mock(Pool)
        mock.uuid = '00000000-0000-0000-0000-123456789123'

        task = Task(None, "name", mock, 1)
        assert task.name == "name" and task._pooluuid == "00000000-0000-0000-0000-123456789123" and task.instancecount

    def test_task_retrieve(self):
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = json.loads('{"name": "name","profile": "docker-batch","instanceCount": 1,"shortname":"name", "poolUuid": null, "resourceBuckets": null, "resultBucket":null, "status": null, "creationDate": "2018-06-13T09:06:20Z", "errors": [], "constants": [],"uuid": "00000000-0000-0000-0000-000000000000", "state": "yes"}')

        mock = Mock(Connection)
        mock._get.return_value = response

        Task._retrieve(mock, "00000000-0000-0000-0000-123456789123")
        mock._get.assert_called_with("/tasks/00000000-0000-0000-0000-123456789123")

    def test_task_retrieve_error_404(self):
        response = requests.Response()
        response.status_code = 404
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message": "problem"}')

        mock = Mock(Connection)
        mock._get.return_value = response

        with pytest.raises(MissingTaskException):
            Task._retrieve(mock, "00000000-0000-0000-0000-123456789123")
            Task.from_json = MagicMock()
            Task.from_json.return_value = Task(mock, "name", "docker-batch", 1)
    
    def test_task_from_json_create_new_task(self):
        mock = Mock(Connection)

        task = Task.from_json(mock, json.loads('{"name": "name","profile": "docker-batch","instanceCount": 1,"shortname":"name", "poolUuid": null, "resourceBuckets": null, "resultBucket":null, "status": null, "creationDate": "2018-06-13T09:06:20Z", "errors": [], "constants": [],"uuid": "00000000-0000-0000-0000-000000000000", "state": "yes"}'))

        assert task.name == "name" and task.profile == "docker-batch" and task.instancecount == 1 \
               and task._shortname == "name" and task._pooluuid == None and task.resources == [] and task.results == None \
               and task.status == None and task.creation_date == dateutil.parser.parse("2018-06-13 09:06:20"), task.errors == [] \
               and task.constants == {"DOCKER_CMD": "sleep 39"} and task.uuid == "0000000-0000-0000-0000-000000000000" \
               and task.state == "yes"

    def test_task_run(self):
        mockconn = Mock(Connection)
        task = Task(mockconn, "name", "docker-batch", 1)
        task.submit = MagicMock()
        task.wait = MagicMock()
        task.abort = MagicMock()
        task.download_results = MagicMock()

        task.run()
        assert task.submit.called                   \
               and task.wait.called                 \
               and not task.abort.called            \
               and not task.download_results.called

    def test_task_run_abort(self):
        mockconn = Mock(Connection)
        task = Task(mockconn, "name", "docker-batch", 1)
        task.submit = MagicMock()
        task.wait = MagicMock()
        task.abort = MagicMock()
        task.download_results = MagicMock()

        task.run(job_timeout=32)
        assert task.submit.called                   \
               and task.wait.called                 \
               and task.abort.called                \
               and not task.download_results.called

    def test_task_run_download(self):
        mockconn = Mock(Connection)
        task = Task(mockconn, "name", "docker-batch", 1)
        task.submit = MagicMock()
        task.wait = MagicMock()
        task.abort = MagicMock()
        task.download_results = MagicMock()

        task.run(output_dir=".")
        assert task.submit.called                   \
               and task.wait.called                 \
               and not task.abort.called                \
               and task.download_results.called

    def test_task_resume(self):
        mockconn = Mock(Connection)
        task = Task(mockconn, "name", "docker-batch", 1)
        task.wait = MagicMock()
        task.abort = MagicMock()
        task.download_results = MagicMock()

        res = task.resume("dehors")
        assert res == "dehors"

    def test_task_resume_uuid(self):
        mockconn = Mock(Connection)
        task = Task(mockconn, "name", "docker-batch", 1)
        task.wait = MagicMock()
        task.abort = MagicMock()
        task.download_results = MagicMock()
        task._uuid = "00000000-0000-0000-0000-123456789123"

        res = task.resume("dehors")
        assert res == None and task.wait.called and task.download_results.called_with("dehors")

    def test_task_submit_usual(self):
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = json.loads('{"name": "name","profile": "docker-batch","instanceCount": 1, "uuid": "00000000-0000-0000-0000-000000000000"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        task.submit()

        assert task.uuid == "00000000-0000-0000-0000-000000000000" and task.resources[0].flush.called

    def test_task_submit_already_submited(self):
        mockconn = Mock(Connection)

        task = Task(mockconn, "name", "docker-batch", 1)
        task._uuid = "00000000-0000-0000-0000-000000000000"
        task._state = "yeah"
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        res = task._pre_submit()
        assert res == "yeah" and not task.resources[0].flush.called

    def test_task_submit_error_404(self):
        response = requests.Response()
        response.status_code = 404
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message": "Task doesn\'t exist"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        with pytest.raises(MissingDiskException):
            task.submit()
            assert task.uuid == None and task.resources[0].flush.called

    def test_task_submit_error_403_max_disk_reached(self):
        response = requests.Response()
        response.status_code = 403
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message": "Maximum number of disks reached :)"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        with pytest.raises(MaxDiskException):
            task.submit()
            assert task.uuid == None and task.resources[0].flush.called
    
    def test_task_submit_error_403_max_task_reached(self):
        response = requests.Response()
        response.status_code = 403
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message": "Maximum number of Tasks reached :)"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        with pytest.raises(MaxTaskException):
            task.submit()
            assert task.uuid == None and task.resources[0].flush.called

    def test_task_submit_error_402_not_enough_credits(self):
        response = requests.Response()
        response.status_code = 402
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message": "Not enough credits"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.resources = [Mock(Bucket)]
        task._resource_type = Bucket
        task.update = MagicMock()
        task.update.return_value = True

        with pytest.raises(NotEnoughCreditsException):
            task.submit()
            assert task.uuid == None and task.resources[0].flush.called
    
    def test_task_abort(self):
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message":"OK"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.update = MagicMock()
        task.update.return_value = True
        task._uuid = "00000000-0000-0000-0000-123456789123"

        task.abort()

        assert task.update.called and mockconn._post.called_with("/tasks/00000000-0000-0000-0000-123456789123/abort")

    def test_task_abort_task_does_not_exist(self):
        response = requests.Response()
        response.status_code = 404
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message":"Task doesn\'t exist"}')

        mockconn = Mock(Connection)
        mockconn._post.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.update = MagicMock()
        task.update.return_value = True
        task._uuid = "00000000-0000-0000-0000-123456789123"

        with pytest.raises(MissingTaskException):
            task.abort()
            assert task.update.called and mockconn._post.called_with("/tasks/00000000-0000-0000-0000-123456789123/abort")

    def test_task_update(self):
        js = json.loads('{"name": "name","profile": "docker-batch","instanceCount": 1,"shortname":"name", "poolUuid": null, "resourceBuckets": null, "resultBucket":null, "status": null, "creationDate": "2018-06-13T09:06:20Z", "errors": [], "constants": [{"DOCKER_CMD": "sleep 39"}],"uuid": "00000000-0000-0000-0000-000000000000", "state": "yes"}')
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = js

        mockconn = Mock(Connection)
        mockconn._get.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task._uuid = "00000000-0000-0000-0000-123456789123"
        task.update(True)

        assert task.name == "name" and task.profile == "docker-batch" and task.instancecount == 1 \
               and task.shortname == "name" and task._pooluuid == None and task.resources == [] and task.results == None \
               and task.status == None and task.creation_date == dateutil.parser.parse("2018-06-13 09:06:20"), task.errors == [] \
               and task.constants == {"DOCKER_CMD": "sleep 39"} and task.uuid == "0000000-0000-0000-0000-000000000000" \
               and task.state == "yes"

    def test_task_update_error_404(self):
        js = json.loads('{"message":"problem"}')
        response = requests.Response()
        response.status_code = 404
        response.json = MagicMock()
        response.json.return_value = js

        mockconn = Mock(Connection)
        mockconn._get.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task._uuid = "00000000-0000-0000-0000-123456789123"

        with pytest.raises(MissingTaskException):
            task.update(True)

    def test_task_delete_with_uuid_no_resources(self):
        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message":"OK"}')
        mockconn = Mock(Connection)
        mockconn._delete.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.update = MagicMock()
        task.update.return_value = True
        task._uuid = "00000000-0000-0000-0000-123456789123"

        task.delete()
        assert task.uuid == None and task.state == "Deleted" and task.resources == [] and task.results == None and mockconn._delete.called_with("/tasks/00000000-0000-0000-0000-123456789123")

    def test_task_delete_with_invalid_uuid(self):
        response = requests.Response()
        response.status_code = 404
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message":"not found"}')

        mockconn = Mock(Connection)
        mockconn._delete.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task.update = MagicMock()
        task.update.return_value = True
        task._uuid = "lkjwefwe"

        with pytest.raises(MissingTaskException):
            task.delete()

    """
    This tests needs s3 mocking integration to use buckets (still in study)

    def test_task_delete_with_uuid_resources(self):

        response = requests.Response()
        response.status_code = 200
        response.json = MagicMock()
        response.json.return_value = json.loads('{"message":"OK"}')

        mockconn = Mock(Connection)
        mockconn._delete.return_value = response

        bucket = Bucket(mockconn, "00000000-0000-0000-0000-123456789123")

        task = Task(mockconn, "name", "docker-batch", 1)
        task.update = MagicMock()
        task.update.return_value = True
        task._resource_type = Bucket
        task._uuid = "00000000-0000-0000-0000-123456789123"
        task.resources.append(bucket)
        task._resource_objects_ids.append(bucket.uuid)

        task.delete(purge_resources=True)
        time.sleep(1)
        assert task.uuid == None and task.state == "Deleted" and task.resources == [] and task.results == None and mockconn._delete.called_with("/tasks/00000000-0000-0000-0000-123456789123")
    """

    def test_task_delete_without_uuid(self):
        response = requests.Response()
        response.status_code = 200

        mockconn = Mock(Connection)
        mockconn._delete.return_value = response

        task = Task(mockconn, "name", "docker-batch", 1)
        task._state = "Active"
        task.update = MagicMock()
        task.update.return_value = True

        task.delete()
        assert task.uuid == None and task.state == "Active" and task.resources == [] and task.results == None and not mockconn._delete.called