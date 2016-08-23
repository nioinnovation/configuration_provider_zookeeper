import unittest
from unittest.mock import Mock, patch, MagicMock

from nio.modules.settings import Settings
from niocore.configuration import Configuration
from niocore.testing.test_case import NIOCoreTestCase

try:
    import kazoo
    from ..provider import ZookeeperConfigurationProvider
    from ..provider import ZookeeperProxy
    ZookeeperProxy_namespace = "{}.ZookeeperProxy".format(
        ZookeeperConfigurationProvider.__module__)
    kazoo_installed = True
except ImportError:
    kazoo_installed = False


class MyZookeeperProxy(object):

    def __init__(self):
        self.connect = Mock()
        self.get_root_path = MagicMock(return_value="/nio_configuration")
        self._data = {}

    def get_children(self, node_path):
        pass

    def register(self, node_path, config):
        self.save(node_path, config)

    def fetch(self, node_path):
        if node_path in self._data:
            return self._data[node_path]

        return {}

    def save(self, node_path, config):
        self._data[node_path] = config

    def remove(self, node_path):
        del self._data[node_path]


@unittest.skipUnless(kazoo_installed, "kazoo is not installed")
class TestZookeeperProvider(NIOCoreTestCase):

    def get_test_configuration_provider(self):
        return ZookeeperConfigurationProvider

    def setUp(self):
        super().setUp()
        ZookeeperConfigurationProvider.reset()

    def _get_settings(self):
        return Settings.get()

    @patch(ZookeeperProxy_namespace)
    def test_one_proxy_reference(self, proxy_mock):
        """ Asserts one proxy usage

        Two instances hold a reference to same proxy, and only one
        connection is established
        """
        my_proxy = MyZookeeperProxy()
        proxy_mock.return_value = my_proxy

        settings = self._get_settings()
        provider1 = ZookeeperConfigurationProvider(settings)
        provider2 = ZookeeperConfigurationProvider(settings)

        self.assertEqual(provider1._get_proxy(), provider2._get_proxy())
        self.assertEqual(my_proxy.connect.call_count, 1)

    @patch(ZookeeperProxy_namespace)
    def test_does_not_exist(self, proxy_mock):
        """ Asserts that fetching from a non-existent returns Empty conf.
        """
        my_proxy = MyZookeeperProxy()
        proxy_mock.return_value = my_proxy

        provider = ZookeeperConfigurationProvider(self._get_settings())
        # fetching data that does not exist, returns empty Configuration
        config = provider.fetch("does_not_exist")
        self.assertTrue(isinstance(config, Configuration))

    @patch(ZookeeperProxy_namespace)
    def test_mappings(self, proxy_mock):
        """ Asserts that mapping to node_path is applied effectively
        """
        mappings = {"modules": "modules_id", "default": "default_id"}

        my_proxy = MyZookeeperProxy()
        # override register method and do own checking
        my_proxy.register = self._mappings_register
        proxy_mock.return_value = my_proxy

        settings = self._get_settings()
        settings.providers["mappings"] = mappings
        provider = ZookeeperConfigurationProvider(settings)

        # test 'modules' mapping register call
        expected_path_data = '{0}/{1}/modules/module1'.format(
            my_proxy.get_root_path(), mappings["modules"])

        config = Configuration(name="modules")
        sub_config = Configuration(name="module1",
                                   data={"expected_path": expected_path_data})
        provider.register(config, sub_config, "module1")

        # test 'modules' mapping register call
        expected_path_data = '{0}/{1}/blocks/simulate'.format(
            my_proxy.get_root_path(), mappings["default"])

        config = Configuration(name="blocks")
        sub_config = Configuration(name="simulate",
                                   data={"expected_path": expected_path_data})
        provider.register(config, sub_config, "simulate")

    def _mappings_register(self, node_path, config):
        self.assertEqual(node_path, config["expected_path"])

    @patch(ZookeeperProxy_namespace)
    def test_save_fetch(self, proxy_mock):
        """ Asserts that fetching a previously saved data works
        """

        my_proxy = MyZookeeperProxy()
        proxy_mock.return_value = my_proxy

        provider = ZookeeperConfigurationProvider(self._get_settings())

        config = Configuration(name="config_name",
                               data={"entry1": "entry1_data"})
        provider.save(config)
        data_fetched = provider.fetch("config_name")
        self.assertEqual(data_fetched["entry1"], "entry1_data")

    @patch(ZookeeperProxy_namespace)
    def test_remove(self, proxy_mock):
        """ Asserts that remove works as expected
        """

        my_proxy = MyZookeeperProxy()
        proxy_mock.return_value = my_proxy

        provider = ZookeeperConfigurationProvider(self._get_settings())

        config = Configuration(name="config_name",
                               data={"entry1": "entry1_data"})
        provider.save(config)
        data_fetched = provider.fetch("config_name")
        self.assertEqual(data_fetched["entry1"], "entry1_data")

        provider.remove(config)
        data_fetched = provider.fetch("config_name")
        self.assertNotIn("entry1", data_fetched)
