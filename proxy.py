import atexit
import json

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsError, NoNodeError

from niocore.util.hooks import Hooks


class ZookeeperProxy(object):

    hook_points = ['kazoo_state_change']

    def __init__(self):
        self._zk = None
        self.logger = None
        self._root_path = None
        self._hooks = Hooks(ZookeeperProxy.hook_points)

    def listener(self, state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            self.logger.info("listener, KazooState.LOST")
        elif state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            self.logger.info("listener, KazooState.SUSPENDED")
        elif state == KazooState.CONNECTED:
            # Handle being connected/reconnected to Zookeeper
            self.logger.info("listener, KazooState.CONNECTED")
        else:
            self.logger.info("listener, KazooState unknown")

        self.hooks.run('kazoo_state_change', state)

    def connect(self, ip_address, port, root_path, logger):
        if not self._zk:
            # establish zookeeper connection
            self._zk = KazooClient(hosts='{0}:{1}'.format(
                ip_address, port, logger=logger))
            self._zk.start()
            self._zk.add_listener(self.listener)

            # Ensure a path, create if necessary
            self._root_path = root_path
            self._zk.ensure_path(self._root_path)

            self.logger = logger
            # make sure finalize is called when stopping nio
            atexit.register(self.disconnect)

    def disconnect(self):
        self.logger.info("Disconnecting")
        if self._zk:
            self._zk.stop()
            self._zk = None

    def get_children(self, node_path):
        try:
            children = self._zk.get_children(node_path)
            return children
        except NoNodeError:
            pass  # pragma: no cover

        return None

    def fetch(self, node_path):
        try:
            data, stat = self._zk.get(node_path)
            if data:
                data = json.loads(data.decode())
        except NoNodeError:
            data = {}  # pragma: no cover
        return data

    def register(self, node_path, config):
        serialized_config = self._process_for_serialization(config)
        try:
            self._zk.create(node_path, serialized_config)
        except NodeExistsError:
            self._zk.set(node_path, serialized_config)

    def save(self, node_path, config):
        self._zk.set(node_path, self._process_for_serialization(config))

    def remove(self, node_path):
        self._zk.delete(node_path, recursive=True)

    @staticmethod
    def _process_for_serialization(config):
        data = {k: config[k] for k in config if not k.startswith('_')}
        return json.dumps(data).encode()

    def get_root_path(self):
        return self._root_path

    @property
    def hooks(self):
        return self._hooks
