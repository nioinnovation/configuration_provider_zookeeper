import unittest
from unittest.mock import Mock, patch

try:
    import kazoo
    from ..provider import ZookeeperProxy
    kazoo_installed = True
except:
    kazoo_installed = False

from niocore.testing.test_case import NIOCoreTestCaseNoModules


class MyKazooClient(object):
    def __init__(self):
        self.start = Mock()
        self.add_listener = Mock()
        self.ensure_path = Mock()
        self.stop = Mock()
        self._data_set = None

    def set(self, _, data):
        self._data_set = data

    def get(self, _):
        return self._data_set, "stat"


@unittest.skipUnless(kazoo_installed, "kazoo is not installed")
class TestZookeeperProxy(NIOCoreTestCaseNoModules):

    def setUp(self):
        super().setUp()
        import logging
        logging.basicConfig()
        self.logger = logging.getLogger('basic')

    @patch(ZookeeperProxy.__module__ + ".KazooClient")
    def test_connect_disconnect(self, kazoo_client_mock):

        """ Asserts that connect/disconnect setup and cleanup as expected
        """

        zk = ZookeeperProxy()
        kazoo_client = MyKazooClient()
        kazoo_client_mock.return_value = kazoo_client

        self.assertFalse(kazoo_client.start.called)
        self.assertFalse(kazoo_client.add_listener.called)
        self.assertFalse(kazoo_client.ensure_path.called)
        zk.connect("ip_address", 2181, "unused_path", self.logger)
        self.assertTrue(kazoo_client.start.called)
        self.assertTrue(kazoo_client.add_listener.called)
        self.assertTrue(kazoo_client.ensure_path.called)
        self.assertEqual(kazoo_client.start.call_count, 1)
        self.assertEqual(kazoo_client.add_listener.call_count, 1)
        self.assertEqual(kazoo_client.ensure_path.call_count, 1)

        # connecting more than once is ignored
        zk.connect("ip_address", 2181, "unused_path", self.logger)
        self.assertEqual(kazoo_client.start.call_count, 1)
        self.assertEqual(kazoo_client.add_listener.call_count, 1)
        self.assertEqual(kazoo_client.ensure_path.call_count, 1)

        self.assertFalse(kazoo_client.stop.called)
        zk.disconnect()

        self.assertTrue(kazoo_client.stop.called)
        self.assertEqual(kazoo_client.stop.call_count, 1)

        # disconnecting more than once is ignored
        self.assertEqual(kazoo_client.stop.call_count, 1)

    @patch(ZookeeperProxy.__module__ + ".KazooClient")
    def test_save_fetch(self, kazoo_client_mock):
        """ Asserts that save and fetch are in sync

        Zookeeper expects data in bytes, here we make sure that happens and
        that data is retrieved to the proxy and transformed as expected
        """

        zk = ZookeeperProxy()

        kazoo_client = MyKazooClient()
        kazoo_client_mock.return_value = kazoo_client

        zk.connect("ip_address", 2181, "unused_path", self.logger)

        data = {"member1": "member1_data"}

        zk.save("node_path", data)
        data_retrieved = zk.fetch("node_path")

        # assert that data sent to zookeeper is in bytes
        self.assertTrue(isinstance(kazoo_client._data_set, bytes))

        # assert that data retrieved has been transformed back.
        self.assertEqual(data_retrieved, data)
