import boto3
import botocore
import dateutil
import json
import os
import pytest
import requests
import time

from qarnot.bucket import Bucket
from qarnot.connection import Connection
from qarnot.exceptions import MissingDiskException, MissingTaskException, MaxDiskException, MaxTaskException, NotEnoughCreditsException
from qarnot.pool import Pool
from qarnot.task import Task
from qarnot.storage import Storage
from unittest.mock import Mock, MagicMock, call, patch

class TestStorage:
    def test_get_files_no_output_specified(self):
        storage = Storage()
        storage._download_file = MagicMock()
        
        storage.get_file("input/lorem.txt")

        storage._download_file.assert_called_with("input/lorem.txt", "lorem.txt", None)

    def test_get_files_local_dir_specified(self):
        os.makedirs("quelque/part/voila/")
        storage = Storage()
        storage._download_file = MagicMock()
                
        storage.get_file("input/lorem.txt", "quelque/part/voila/")

        storage._download_file.assert_called_with("input/lorem.txt", "quelque/part/voila/lorem.txt", None)
        os.removedirs("quelque/part/voila/")
        
    def test_get_files_local_file_specified(self):
        os.makedirs("quelque/part/voila/")
        storage = Storage()
        storage._download_file = MagicMock()
                
        storage.get_file("input/lorem.txt", "quelque/part/voila/file.jsp")

        storage._download_file.assert_called_with("input/lorem.txt", "quelque/part/voila/file.jsp", None)
        os.removedirs("quelque/part/voila/")
        
    def test_get_files_progress_True(self):
        storage = Storage()
        storage._download_file = MagicMock()
                
        storage.get_file("input/lorem.txt", progress=True)

        storage._download_file.assert_called_with("input/lorem.txt", "lorem.txt", True)

    def test_storage_equality_True(self):
        Storage1 = Storage()
        Storage2 = Storage()
        
        Storage1._uuid = "0-0-0-0-0"
        Storage2._uuid = "0-0-0-0-0"

        assert Storage1 == Storage2

    def test_storage_equality_False(self):
        Storage1 = Storage()
        Storage2 = Storage()
        
        Storage1._uuid = "0-0-0-0-0"
        Storage2._uuid = "0-0-0-0-1"

        assert not (Storage1 == Storage2)

    def test_storage_inequality_True(self):
        Storage1 = Storage()
        Storage2 = Storage()
        
        Storage1._uuid = "0-0-0-0-0"
        Storage2._uuid = "0-0-0-0-1"

        assert Storage1 != Storage2

    def test_storage_inequality_False(self):
        Storage1 = Storage()
        Storage2 = Storage()
        
        Storage1._uuid = "0-0-0-0-0"
        Storage2._uuid = "0-0-0-0-0"

        assert not (Storage1 != Storage2)

    def test_get_item_from_storage(self):
        storage = Storage()
        storage.get_file = MagicMock()

        storage["input/lorem.txt"]

        storage.get_file.assert_called_with("input/lorem.txt")

    def test_get_all_files_from_storage(self):

        mock1 = Mock()
        mock1.key = "file1"
        mock2 = Mock()
        mock2.key = "file2"
        mock3 = Mock()
        mock3.key = "nested/file3"

        os.mkdir("output")
        storage = Storage()
        storage.get_file = MagicMock()
        storage.list_files = MagicMock()
        storage.list_files.return_value = [mock1, mock2, mock3] 

        storage.get_all_files("output")

        os.removedirs("output")

        assert storage.get_file.call_count == 3
        storage.get_file.assert_any_call("file1", "output/file1", None)
        storage.get_file.assert_any_call("file2", "output/file2", None)
        storage.get_file.assert_any_call("nested/file3", "output/nested/file3", None)
