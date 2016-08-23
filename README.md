# nio zookeeper configuration provider

A nio configuration provider using zookeeper


## Configuration

[providers]

- specify ZookeeperConfigurationProvider overriding default type
type: niocore.configuration.providers..zookeeper.ZookeeperConfigurationProvider

- ip_address and port to zookeeper server
ip_address: 127.0.0.1
port: 2181

- zookeeper root path for configuration data
root_path=nio_configuration

- mappings allow sharing configurations among instances at different levels
mappings: {"default": 1}

## Dependencies

-   [kazoo](https://pypi.python.org/pypi/kazoo)
