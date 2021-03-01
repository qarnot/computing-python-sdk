"""Module for bucket object."""

# Copyright 2017 Qarnot computing
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

import hashlib
import sys
import os
import posixpath
import shutil
import itertools

from boto3.s3.transfer import TransferConfig
from itertools import groupby
from operator import attrgetter

from . import _util
from .exceptions import BucketStorageUnavailableException
from .storage import Storage

# Max size in bytes before uploading in parts.

AWS_UPLOAD_MAX_SIZE = 8 * 1024 * 1024
# Size of parts when uploading in parts
AWS_UPLOAD_PART_SIZE = 8 * 1024 * 1024

s3_multipart_config = TransferConfig(
    multipart_threshold=AWS_UPLOAD_MAX_SIZE,
    multipart_chunksize=AWS_UPLOAD_PART_SIZE,
    max_concurrency=10,
    num_download_attempts=10,
)


class Bucket(Storage):
    """Represents a resource/result bucket.

    This class is the interface to manage resources or results from a
    :class:`qarnot.bucket.Bucket`.

    .. note::
       A :class:`Bucket` must be created with
       :meth:`qarnot.connection.Connection.create_bucket`
       or retrieved with :meth:`qarnot.connection.Connection.buckets`, :meth:`qarnot.connection.Connection.retrieve_bucket`,
       or :meth:`qarnot.connection.Connection.retrieve_or_create_bucket`.

    .. note::
       Paths given as 'remote' arguments,
       (or as path arguments for :func:`Bucket.directory`)
       **must** be valid unix-like paths.
    """

    def __init__(self, connection, name, create=True):
        if connection.s3client is None:
            raise BucketStorageUnavailableException()

        self._connection = connection
        self._uuid = name

        if create:
            self._connection.s3client.create_bucket(Bucket=name)

    @classmethod
    def _retrieve(cls, connection, bucket_uuid):
        """Retrieve information of a bucket on a cluster.

        :param :class:`qarnot.connection.Connection` connection: the cluster
            to get the bucket from
        :param str bucket_uuid: the UUID of the bucket to retrieve

        :rtype: :class:`qarnot.bucket.Bucket`
        :returns: The retrieved bucket.

        :raises qarnot.exceptions.BucketStorageUnavailableException: the bucket storage engine is not available
        """
        return connection.retrieve_bucket(uuid=bucket_uuid)

    def delete(self):
        """ Delete the bucket represented by this :class:`Bucket`."""

        n = 1000  # delete object count max request

        bucket = self._connection.s3resource.Bucket(self._uuid)
        versioned_bucket = self._connection.s3resource.BucketVersioning(self._uuid)

        if versioned_bucket.status == 'None':
            objectlist = list(bucket.objects.all())
            if sys.version_info >= (3, 0):
                listofobjectlist = [[{'Key': x.key} for x in objectlist[i:i + n]] for i in range(0, len(objectlist), n)]
            else:
                # noinspection PyUnresolvedReferences
                listofobjectlist = [[{'Key': x.key} for x in objectlist[i:i + n]] for i in xrange(0, len(objectlist), n)]  # noqa
        else:
            objectlist = list(bucket.object_versions.all())
            if sys.version_info >= (3, 0):
                listofobjectlist = [[{'Key': x.key, 'VersionId': x.id} for x in objectlist[i:i + n]] for i in range(0, len(objectlist), n)]
            else:
                # noinspection PyUnresolvedReferences
                listofobjectlist = [[{'Key': x.key, 'VersionId': x.id} for x in objectlist[i:i + n]] for i in xrange(0, len(objectlist), n)]  # noqa

        for item in listofobjectlist:
            bucket.delete_objects(
                Delete={
                    'Objects': item
                }
            )
        self._connection.s3client.delete_bucket(Bucket=self._uuid)

    def list_files(self):
        """List files in the bucket

        :rtype: list(:class:`S3.ObjectSummary`)
        :returns: A list of ObjectSummary resources

        """
        bucket = self._connection.s3resource.Bucket(self._uuid)
        return bucket.objects.all()

    def directory(self, directory=''):
        """List files in a directory of the bucket according to prefix.

        :rtype: list(:class:`S3.ObjectSummary`)
        :returns: A list of ObjectSummary resources
        """
        bucket = self._connection.s3resource.Bucket(self._uuid)
        return bucket.objects.filter(Prefix=directory)

    def sync_directory(self, directory, verbose=False, remote=None):
        """Synchronize a local directory with the remote buckets.

        :param str directory: The local directory to use for synchronization
        :param bool verbose: Print information about synchronization operations
        :param str remote: path of the directory on remote node (defaults to *local*)

        .. warning::
           Local changes are reflected on the server, a file present on the
           bucket but not in the local directory will be deleted from the bucket.

           A file present in the directory but not in the bucket will be uploaded.

        .. note::
           The following parameters are used to determine whether
           synchronization is required :

              * name
              * size
              * sha1sum
        """
        if not directory.endswith(os.sep):
            directory += os.sep

        filesdict = {}
        for root, _, files in os.walk(directory):
            root = _util.decode(root)
            files = map(_util.decode, files)

            for file_ in files:
                filepath = os.path.join(root, file_)
                name = filepath[len(directory):]
                name = name.replace(os.sep, '/')
                filesdict[name] = filepath

        self.sync_files(filesdict, verbose, remote)

    def sync_files(self, files, verbose=False, remote=None):
        """Synchronize files  with the remote buckets.

        :param dict files: Dictionary of synchronized files
        :param bool verbose: Print information about synchronization operations
        :param str remote: path of the directory on remote node (defaults to *local*)

        Dictionary key is the remote file path while value is the local file
        path.

        .. warning::
           Local changes are reflected on the server, a file present on the
           bucket but
           not in the local directory will be deleted from the bucket.

           A file present in the directory but not in the bucket will be uploaded.

        .. note::
           The following parameters are used to determine whether
           synchronization is required :

              * name
              * size
              * sha1sum
        """

        class Comparable(object):
            def __init__(self, name_, e_tag, filepath_):
                self.name = name_
                self.e_tag = e_tag
                self.filepath = filepath_

            def __repr__(self):
                return "Name {0}, ETag {1}".format(self.name, self.e_tag)

            def __eq__(self, other):
                return self.name == other.name and self.e_tag == other.e_tag

            def __hash__(self):
                return hash(self.name) ^ hash(self.e_tag)

        def aws_md5sum(sourcepath):
            if os.stat(sourcepath).st_size < AWS_UPLOAD_MAX_SIZE:
                hash_md5 = hashlib.md5()
                with open(sourcepath, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                return "\"{0}\"".format(hash_md5.hexdigest())
            else:
                md5s = []
                with open(sourcepath, 'rb') as fp:
                    while True:

                        data = fp.read(AWS_UPLOAD_PART_SIZE)

                        if not data:
                            break
                        md5s.append(hashlib.md5(data))

                digests = b"".join(m.digest() for m in md5s)

                new_md5 = hashlib.md5(digests)
                return "\"{0}-{1}\"".format(new_md5.hexdigest(), len(md5s))

        def localtocomparable(name_, filepath_, remote):
            if remote is not None:
                name_ = os.path.join(remote, name_.lstrip('/'))
            return Comparable(name_.replace(os.sep, '/'), aws_md5sum(filepath_), filepath_)

        def objectsummarytocomparable(object_):
            return Comparable(object_.key, object_.e_tag, None)

        localfiles = set()
        for name, filepath in files.items():
            localfiles.add(localtocomparable(name.replace(os.path.sep, '/'), filepath, remote))

        remotefiles = set(map(objectsummarytocomparable, self.list_files()))

        adds = localfiles - remotefiles
        removes = remotefiles - localfiles

        for file_ in removes:
            if remote is not None and not file_.name.startswith(remote):
                continue
            renames = (x for x in adds if x.e_tag == file_.e_tag and all(rem.name != x.name for rem in remotefiles))
            for dup in renames:
                if verbose:
                    print("Copy", file_.name, "to", dup.name)
                self.copy_file(file_.name, dup.name)
            if verbose:
                print("Remove:", file_.name)
            self.delete_file(file_.name)

        remotefiles = set(map(objectsummarytocomparable, self.list_files()))

        sadds = sorted(adds, key=lambda x: x.e_tag)
        groupedadds = (list(g) for _, g in itertools.groupby(sadds, lambda x: x.e_tag))

        for entry in groupedadds:
            try:
                rem = next(x for x in remotefiles if x.e_tag == entry[0].e_tag)
                if rem.name == entry[0].name:
                    continue
                if verbose:
                    print("Copy", rem.name, "to", entry[0].name)
                self.copy_file(rem.name, entry[0].name)
            except StopIteration:
                if verbose:
                    print("Upload:", entry[0].filepath, '->', entry[0].name)
                self.add_file(entry[0].filepath, entry[0].name)
            for link in entry[1:]:  # duplicate files
                if verbose:
                    print("Copy", entry[0].name, "to", link.name)
                self.copy_file(entry[0].name, link.name)

    @_util.copy_docs(Storage.add_file)
    def add_file(self, local_or_file, remote=None):
        tobeclosed = False
        if _util.is_string(local_or_file):
            file_ = open(local_or_file, 'rb')
            tobeclosed = True
        else:
            file_ = local_or_file
        dest = remote or os.path.basename(file_.name)

        self._connection.s3client.upload_fileobj(file_, self._uuid, dest, Config=s3_multipart_config)
        if tobeclosed:
            file_.close()

    @_util.copy_docs(Storage.get_all_files)
    def get_all_files(self, output_dir, progress=None):
        list_files_only = [x for x in self.list_files() if not x.key.endswith('/')]
        list_directories_only = [x for x in self.list_files() if x.key.endswith('/')]

        for dir in list_directories_only:
            if not os.path.isdir(os.path.join(output_dir, dir.key.lstrip('/'))):
                os.makedirs(os.path.join(output_dir, dir.key.lstrip('/')))

        for _, dupes in groupby(sorted(list_files_only, key=attrgetter('e_tag')), attrgetter('e_tag')):
            file_info = next(dupes)
            first_file = os.path.join(output_dir, file_info.key.lstrip('/'))
            self.get_file(file_info.get()['Body'], local=first_file)  # avoids making a useless HEAD request

            for dupe in dupes:
                local = os.path.join(output_dir, dupe.key.lstrip('/'))
                directory = os.path.dirname(local)
                if not os.path.exists(directory):
                    os.makedirs(directory)
                if (os.path.abspath(os.path.realpath(local)) is not os.path.abspath(os.path.realpath(first_file))):
                    shutil.copy(first_file, local)

    @_util.copy_docs(Storage.get_file)
    def get_file(self, remote, local=None, progress=None):
        return super(Bucket, self).get_file(remote, local, progress)

    @_util.copy_docs(Storage.add_directory)
    def add_directory(self, local, remote=""):
        if not os.path.isdir(local):
            raise IOError("Not a valid directory")
        if remote and not remote.endswith('/'):
            remote += '/'
        for dirpath, _, files in os.walk(local):
            dirpath = _util.decode(dirpath)
            files = map(_util.decode, files)

            remote_loc = dirpath.replace(local, remote, 1)
            remote_loc = remote_loc.replace('\\', '/')
            for filename in files:
                self.add_file(os.path.join(dirpath, filename),
                              posixpath.join(remote_loc, filename))

    @_util.copy_docs(Storage.copy_file)
    def copy_file(self, source, dest):
        copy_source = {
            'Bucket': self._uuid,
            'Key': source
        }
        return self._connection.s3client.copy_object(CopySource=copy_source, Bucket=self._uuid, Key=dest)

    @_util.copy_docs(Storage.flush)
    def flush(self):
        pass

    @_util.copy_docs(Storage.update)
    def update(self, flush=False):
        pass

    def _download_file(self, remote, local, progress=None):
        with open(local, 'wb') as data:
            if hasattr(remote, 'read'):
                shutil.copyfileobj(remote, data)
            else:
                self._connection.s3client.download_fileobj(self._uuid, remote, data)
        return local

    @_util.copy_docs(Storage.delete_file)
    def delete_file(self, remote):
        self._connection.s3client.delete_object(Bucket=self._uuid, Key=remote)

    @property
    def uuid(self):
        """ Bucket identifier"""
        return self._uuid

    @property
    def description(self):
        """ Bucket identifier"""
        return self._uuid
