"""Module describing a connection."""

from qarnot import get_url, raise_on_error
from qarnot.disk import Disk, MissingDiskException
from qarnot.task import Task, MissingTaskException
from qarnot.notification import Notification, TaskCreated, TaskEnded, TaskStateChanged
import requests
import sys
from json import dumps as json_dumps
from requests.exceptions import ConnectionError
if sys.version_info[0] >= 3:  # module renamed in py3
    import configparser as config  # pylint: disable=import-error
else:
    import ConfigParser as config  # pylint: disable=import-error


#########
# class #
#########

class QApy(object):
    """Represents the couple cluster/user to which submit tasks.
    """
    def __init__(self, conf):
        """Create a connection to a cluster with given config file or
        dictionary.

        :param conf: path to a qarnot configuration file or dictionary
          containing following keys:
        * cluster_url
        * cluster_unsafe   (optional)
        * cluster_timeout  (optional)
        * client_auth

        Configuration sample:

        .. code-block:: ini

           [cluster]
           # url of the REST API
           url=https://localhost
           # No SSL verification ?
           unsafe=False
           # timeout put on every GET/POST
           timeout=30
           [client]
           # auth string of the client
           auth=login

        """
        self._http = requests.session()

        if isinstance(conf, dict):
            self.cluster = conf.get('cluster_url')
            self._http.headers.update(
                {"Authorization": conf.get('client_auth')})
            self.auth = conf.get('client_auth')
            self.timeout = conf.get('cluster_timeout')
            if conf.get('cluster_unsafe'):
                self._http.verify = False
        else:
            cfg = config.ConfigParser()
            with open(conf) as cfgfile:
                cfg.readfp(cfgfile)

                self.cluster = cfg.get('cluster', 'url')
                self._http.headers.update(
                    {"Authorization": cfg.get('client', 'auth')})
                self.auth = cfg.get('client', 'auth')
                self.timeout = None
                if cfg.has_option('cluster', 'timeout'):
                    self.timeout = cfg.getint('cluster', 'timeout')

                if cfg.has_option('cluster', 'unsafe') \
                   and cfg.getboolean('cluster', 'unsafe'):
                    self._http.verify = False
        self._get('/')

    def _get(self, url, **kwargs):
        """Perform a GET request on the cluster.

        :param str url:
          relative url of the file (according to the cluster url)

        :rtype: :class:`requests.Response`
        :returns: The response to the given request.

        :raises UnauthorizedException: invalid credentials

        .. note:: Additional keyword arguments are passed to the underlying
           :func:`requests.Session.get`.
        """
        while True:
            try:
                ret = self._http.get(self.cluster + url, timeout=self.timeout,
                                     **kwargs)
                if ret.status_code == 401:
                    raise UnauthorizedException(self.auth)
                return ret
            except ConnectionError as exception:
                if str(exception) == "('Connection aborted.', BadStatusLine(\"\'\'\",))":
                    pass
                else:
                    raise

    def _patch(self, url, json=None, **kwargs):
        """perform a PATCH request on the cluster

        :param url: :class:`str`,
          relative url of the file (according to the cluster url)
        :param json: the data to json serialize and post

        :rtype: :class:`requests.Response`
        :returns: The response to the given request.

        :raises UnauthorizedException: invalid credentials

        .. note:: Additional keyword arguments are passed to the underlying
           :attr:`requests.Session.post()`.
        """
        while True:
            try:
                if json is not None:
                    if 'headers' not in kwargs:
                        kwargs['headers'] = dict()
                    kwargs['headers']['Content-Type'] = 'application/json'
                    kwargs['data'] = json_dumps(json)
                ret = self._http.patch(self.cluster + url,
                                       timeout=self.timeout, **kwargs)
                if ret.status_code == 401:
                    raise UnauthorizedException(self.auth)
                return ret
            except ConnectionError as exception:
                if str(exception) == "('Connection aborted.', BadStatusLine(\"\'\'\",))":
                    pass
                else:
                    raise

    def _post(self, url, json=None, *args, **kwargs):
        """perform a POST request on the cluster

        :param url: :class:`str`,
          relative url of the file (according to the cluster url)
        :param json: the data to json serialize and post

        :rtype: :class:`requests.Response`
        :returns: The response to the given request.

        :raises UnauthorizedException: invalid credentials

        .. note:: Additional keyword arguments are passed to the underlying
           :attr:`requests.Session.post()`.
        """
        while True:
            try:
                if json is not None:
                    if 'headers' not in kwargs:
                        kwargs['headers'] = dict()
                    kwargs['headers']['Content-Type'] = 'application/json'
                    kwargs['data'] = json_dumps(json)
                ret = self._http.post(self.cluster + url,
                                      timeout=self.timeout, *args, **kwargs)
                if ret.status_code == 401:
                    raise UnauthorizedException(self.auth)
                return ret
            except ConnectionError as exception:
                if str(exception) == "('Connection aborted.', BadStatusLine(\"\'\'\",))":
                    pass
                else:
                    raise

    def _delete(self, url, **kwargs):
        """Perform a DELETE request on the cluster.

        :param url: :class:`str`,
          relative url of the file (according to the cluster url)

        :rtype: :class:`requests.Response`
        :returns: The response to the given request.

        :raises UnauthorizedException: invalid credentials

        .. note:: Additional keyword arguments are passed to the underlying
          :attr:`requests.Session.delete()`.
        """

        while True:
            try:
                ret = self._http.delete(self.cluster + url,
                                        timeout=self.timeout, **kwargs)
                if ret.status_code == 401:
                    raise UnauthorizedException(self.auth)
                return ret
            except ConnectionError as exception:
                if str(exception) == "('Connection aborted.', BadStatusLine(\"\'\'\",))":
                    pass
                else:
                    raise

    def _put(self, url, json=None, **kwargs):
        """Performs a PUT on the cluster."""
        while True:
            try:
                if json is not None:
                    if 'headers' not in kwargs:
                        kwargs['headers'] = dict()
                    kwargs['headers']['Content-Type'] = 'application/json'
                    kwargs['data'] = json_dumps(json)
                ret = self._http.put(self.cluster + url,
                                     timeout=self.timeout, **kwargs)
                if ret.status_code == 401:
                    raise UnauthorizedException(self.auth)
                return ret
            except ConnectionError as exception:
                if str(exception) == "('Connection aborted.', BadStatusLine(\"\'\'\",))":
                    pass
                else:
                    raise

    def user_info(self):
        """Get information of the current user on the cluster.

        :rtype: :class:`UserInfo`
        :returns: Requested information.

        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        resp = self._get(get_url('user'))
        raise_on_error(resp)
        ret = resp.json()
        return UserInfo(ret)

    def _disks_get(self, global_):
        url_name = 'global disk folder' if global_ else 'disk folder'
        response = self._get(get_url(url_name))
        raise_on_error(response)
        disks = [Disk.from_json(self, data) for data in response.json()]
        return disks

    def disks(self):
        """Get the list of disks on this cluster for this user.

        :rtype: List of :class:`~qarnot.disk.Disk`.
        :returns: Disks on the cluster owned by the user.


        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        return self._disks_get(global_=False)

    def global_disks(self):
        """Get the list of globally available disks on this cluster.

        :rtype: List of :class:`~qarnot.disk.Disk`.
        :returns: Disks on the cluster available for every user.


        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        return self._disks_get(global_=True)

    def tasks(self):
        """Get the list of tasks stored on this cluster for this user.

        :rtype: List of :class:`~qarnot.task.Task`.
        :returns: Tasks stored on the cluster owned by the user.

        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        response = self._get(get_url('tasks'))
        raise_on_error(response)
        return [Task.from_json(self, task, False) for task in response.json()]

    def retrieve_task(self, uuid):
        """Retrieve a :class:`qarnot.task.Task` from its uuid

        :param str uuid: Desired task uuid
        :rtype: :class:`~qapi.task.Task`
        :returns: Existing task defined by the given uuid
        :raises qarnot.task.MissingTaskException: task does not exist
        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """

        response = self._get(get_url('task update', uuid=uuid))
        if response.status_code == 404:
            raise MissingTaskException(response.json()['message'], uuid)
        raise_on_error(response)
        return Task.from_json(self, response.json(), False)

    def retrieve_disk(self, uuid):
        """Retrieve a :class:`~qarnot.disk.Disk` from its uuid

        :param str uuid: Desired disk uuid
        :rtype: :class:`~qapi.disk.Disk`
        :returns: Existing disk defined by the given uuid
        :raises ValueError: no such disk
        :raises qarnot.disk.MissingDiskException: disk does not exist
        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """

        response = self._get(get_url('disk info', name=uuid))
        if response.status_code == 404:
            raise MissingDiskException(response.json()['message'])
        raise_on_error(response)
        return Disk.from_json(self, response.json())

    def create_disk(self, description, force=False, lock=False,
                    global_disk=False):
        """Create a new :class:`~qarnot.disk.Disk`.

        :param str description: a short description of the disk
        :param bool force: delete an old unlocked disk
          if maximum number of disks is reached
        :param bool lock: prevents the disk to be removed
          by a subsequent :meth:`create_disk` with force set to True

        :rtype: :class:`qarnot.disk.Disk`
        :returns: The created :class:`~qarnot.disk.Disk`.

        :raises qarnot.QApyException: API general error, see message for details
        :raises qarnot.connection.UnauthorizedException: invalid credentials
        """
        disk = Disk(self, description, lock, global_disk, force)
        disk.create()
        return disk

    def create_task(self, name, profile, framecount_or_range, force=False):
        """Create a new :class:`~qarnot.task.Task`.

        :param str name: given name of the task
        :param str profile: which profile to use with this task
        :param framecount_or_range: number of frame or range on  which to run task
        :type framecount_or_range: int or str

        :param bool force: remove an old task if the maximum number of allowed
           tasks is reached. Plus, it will delete an old unlocked disk
           if maximum number of disks is reached for resources and results

        :rtype: :class:`~qarnot.task.Task`
        :returns: The created :class:`~qarnot.task.Task`.

        .. note:: See available profiles with :meth:`profiles`.
        """
        return Task(self, name, profile, framecount_or_range, force)

    def create_task_state_changed_notification(self, destination, filterkey, filtervalue, template=None, toregex=None, fromregex=None, stateregex=None):
        """Create a new :class:`qarnot.notification.Notification` with a filter of type :class:`qarnot.notification.TaskStateChanged`.

        :param str destination: e-mail address
        :param str filterkey: key to watch on tasks
        :param str filtervalue: regex to match for the filter key
        :param str template: (optionnal) Template for the notification
        :param str toregex: (optional) Regex to match the "To" value on a state change, default to ".*"
        :param str fromregex: (optional) Regex to match the "From" value on a state change, default to ".*"
        :param str stateregex: (optional) Regex to match the "From" or "To" value on a state change, default to ".*"
        """
        nfilter = TaskStateChanged(template, destination, filterkey, filtervalue, toregex, fromregex, stateregex)
        return Notification._create(self, nfilter)

    def create_task_created_notification(self, destination, filterkey, filtervalue, template=None):
        """Create a new :class:`qarnot.notification.Notification` with a filter of type :class:`qarnot.notification.TaskCreated`.

        :param str destination: e-mail address
        :param str filterkey: key to watch on tasks
        :param str filtervalue: regex to match for the filter key
        :param str template: (optionnal) Template for the notification
        """
        nfilter = TaskCreated(template, destination, filterkey, filtervalue)
        return Notification._create(self, nfilter)

    def create_task_ended_notification(self, destination, filterkey, filtervalue, template=None):
        """Create a new :class:`qarnot.notification.Notification` with a filter of type :class:`qarnot.notification.TaskEnded`.

        :param str destination: e-mail address
        :param str filterkey: key to watch on tasks
        :param str filtervalue: regex to match for the filter key
        :param str template: (optionnal) Template for the notification
        """
        nfilter = TaskEnded(template, destination, filterkey, filtervalue)
        return Notification._create(self, nfilter)

    def notifications(self):
        """Get the list of notifications for the user

        :rtype: List of :class:~qarnot.task.Notification`.
        :returns: List of all notifications belonging to the user
        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        response = self._get(get_url('notification'))
        raise_on_error(response)
        notifications = [Notification(data, self) for data in response.json()]
        return notifications

    def retrieve_notification(self, uuid):
        """Retrieve a :class:~qarnot.notification.Notification` from it's uuid

        :param str uuid: Id of the notification
        :rtype: :class:`~qapi.notification.Notification`
        :returns: Existing notification defined by the given uuid

        :raises qarnot.connection.UnauthorizedException: invalid credentials
        :raises qarnot.QApyException: API general error, see message for details
        """
        url = get_url('notification update', uuid=uuid)
        response = self._get(url)
        raise_on_error(response)
        return Notification(response.json(), self)


###################
# utility Classes #
###################

class UserInfo(object):
    """Information about a qarnot user."""
    def __init__(self, info):
        self.__dict__.update(info)  # DEPRECATED, keep it for old camel case version

        self.disk_count = info['diskCount']
        """:type: :class:`int`

        Number of disks owned by the user."""
        self.max_disk = info['maxDisk']
        """:type: :class:`int`

        Maximum number of disks allowed (resource and result disks)."""
        self.quota_bytes = info['quotaBytes']
        """:type: :class:`int`

        Total storage space allowed for the user's disks (in Bytes)."""
        self.used_quota_bytes = info['usedQuotaBytes']
        """:type: :class:`int`

        Total storage space used by the user's disks (in Bytes)."""
        self.task_count = info['taskCount']
        """:type: :class:`int`

        Total number of tasks belonging to the user."""
        self.max_task = info['maxTask']
        """:type: :class:`int`

        Maximum number of tasks the user is allowed to create."""
        self.running_task_count = info['runningTaskCount']
        """:type: :class:`int`

        Number of tasks currently in 'Submitted' state."""
        self.max_running_task = info['maxRunningTask']
        """:type: :class:`int`

        Maximum number of running tasks."""
        self.max_instances = info['maxInstances']
        """:type: :class:`int`

        Maximum number of frames per task."""
        self.finished_execution_time = info['finishedExecutionTime']
        """:type: :class:`int`

        Finished computation time."""
        self.running_execution_time = info['runningExecutionTime']
        """:type: :class:`int`

        Running computation time."""
        self.total_execution_time = info['totalExecutionTime']
        """:type: :class:`int`

        Total computation time."""


class Profile(object):
    """Information about a profile."""
    def __init__(self, info):
        self.name = info['name']
        """:type: :class:`str`

        Name of the profile."""
        self.constants = tuple((cst['name'], cst['value'])
                               for cst in info['constants'])
        """:type: List of (:class:`str`, :class:`str`)

        List of couples (name, value) representing constants for this profile
        and their default values."""

    def __repr__(self):
        return 'Profile(name=%s, constants=%r}' % (self.name, self.constants)


##############
# Exceptions #
##############

class UnauthorizedException(Exception):
    """Authorization given is not valid."""
    def __init__(self, auth):
        super(UnauthorizedException, self).__init__(
            "invalid credentials : {0}".format(auth))