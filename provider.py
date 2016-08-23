"""
    Zookeeper configuration provider implementation

"""
import json

from nio.util.logging import get_nio_logger
from niocore.configuration.providers import ConfigurationProvider
from niocore.configuration import Configuration
from .proxy import ZookeeperProxy


__all__ = ['ZookeeperConfigurationProvider']


class ZookeeperConfigurationData(object):

    """ Private configuration data

    Stores data specific to the Zookeeper implementation
    """

    def __init__(self, path, multiple):
        self.path = path
        self.multiple = multiple


class ZookeeperConfigurationProvider(ConfigurationProvider):

    """ Zookeeper Configuration provider implementation
    """

    _zk = None
    _mappings = None

    @staticmethod
    def reset():
        ZookeeperConfigurationProvider._zk = None
        ZookeeperConfigurationProvider._mappings = None

    @staticmethod
    def _get_proxy():
        return ZookeeperConfigurationProvider._zk

    @staticmethod
    def _set_proxy(zookeeper_proxy):
        ZookeeperConfigurationProvider._zk = zookeeper_proxy

    @staticmethod
    def _get_mappings():
        return ZookeeperConfigurationProvider._mappings

    @staticmethod
    def _set_mappings(mappings):
        ZookeeperConfigurationProvider._mappings = mappings

    def __init__(self, settings, config_class=Configuration):
        """ Constructor for zookeeper configuration provider

        Args:
            config_class: class to instantiate for configuration data
        """
        super().__init__(settings)
        self._config_class = config_class
        self.logger = get_nio_logger("ZookeeperConfigurationProvider")
        if not self._get_proxy():
            zk = ZookeeperProxy()
            self._parse_mappings(settings.providers.get("mappings",
                                                        {"default": 1}))
            zk.connect(settings.providers.get("ip_address", "127.0.0.1"),
                       settings.providers.get("port", 2181),
                       settings.providers.get("root_path",
                                              "/nio_configuration"),
                       self.logger)
            self._set_proxy(zk)

    def _fetch(self, child_node_path, substitute=True):
        """ Fetches a zookeeper single configuration

        Args:
            child_node_path (str): path to child node

        Returns:
            Configuration: with config values
        """
        data = self._get_proxy().fetch(child_node_path)
        config = \
            self._config_class(fetch_on_create=False,
                               data=data,
                               substitute=substitute)
        config['_private'] = \
            ZookeeperConfigurationData(child_node_path, False)

        return config

    def fetch(self, name, substitute=True):
        """ Fetches a zookeeper base configuration, multiple or single

        Args:
            name (str): node name
            substitute (bool): substitute variables

        Returns:
            Configuration: with config values
        """

        node_path = "{0}/{1}/{2}".format(self._get_proxy().get_root_path(),
                                         self._get_id(name),
                                         name)
        children = self._get_proxy().get_children(node_path)
        if children:
            config = self._config_class(name=name,
                                        fetch_on_create=False,
                                        substitute=substitute)
            config['_private'] = \
                ZookeeperConfigurationData(node_path, True)

            for child in children:
                child_node_path = "{0}/{1}".format(node_path, child)
                config[child] = self._fetch(child_node_path, substitute)
        else:
            config = self._fetch(node_path, substitute)

        return config

    def register(self, config, sub_config, name):
        """Register a configuration as a child.

        Args:
            config (Configuration): The configuration to register to
            sub_config (Configuration): The sub-configuration to enhance with
                the configuration provider data
            name (str): The name under which to register.
        """
        node_path = "{0}/{1}/{2}/{3}".format(self._get_proxy().get_root_path(),
                                             self._get_id(config.name),
                                             config.name,
                                             name)
        sub_config['_private'] = ZookeeperConfigurationData(node_path,
                                                            False)
        self._get_proxy().register(node_path, sub_config)

    def save(self, config):
        """Save the configuration details.

        This method will update the configuration source with its current
        internal configuration state.

        Args:
            config (Configuration): The configuration to save.

        Returns:
            None
        """
        node_path = self._get_node_path(config)
        self._get_proxy().save(node_path, config)

    def remove(self, config):
        node_path = self._get_node_path(config)
        self._get_proxy().remove(node_path)

    def _get_node_path(self, config):
        try:
            node_path = config['_private'].path
        except KeyError:
            node_path = "{0}/{1}/{2}".format(self._get_proxy().get_root_path(),
                                             self._get_id(config.name),
                                             config.name)

        return node_path

    # mappings allow sharing configurations among instances at great
    # granularity levels, for example, it might be desired to have all
    # instances share the "modules" configuration, yet have "blocks" and
    # "services" shared at "cluster" levels
    #
    # Scenario:
    #   - instance1 and instance3 will share everything
    #   - instance2 will share everything except 'block' and 'service'
    # definitions
    #
    #  instance1
    #  mappings: {"modules": 2, "blocks": 3, "services": 4, default": 1}
    #
    #  instance2
    #  mappings: {"modules": 2, "blocks": 5, "services": 6, default": 1}
    #
    #  instance3
    #  mappings: {"modules": 2, "blocks": 3, "services": 4, default": 1}
    #
    @staticmethod
    def _parse_mappings(mappings):
        if not isinstance(mappings, dict):
            mappings = json.loads(mappings)
        ZookeeperConfigurationProvider._mappings = mappings

    def _get_id(self, name):
        """ Maps name to prefix zookeeper sub-path after root path, i.e.,
          /root-path/[mapped id]
        """
        mappings = self._get_mappings()
        if name in mappings:
            return mappings[name]
        else:
            return mappings.get("default", 1)
