
import logging
import os

from niocore.configuration import Configuration
from proxy import ZookeeperProxy

logging.basicConfig()
logger = logging.getLogger('basic')


def get_node_path(root_path):
    return "{0}/integration_tests".format(root_path)

delete_only = False

zk = ZookeeperProxy()
zk.connect("127.0.0.1", 2181, os.getcwd(), logger)

try:
    children = zk.get_children(get_node_path(zk.get_root_path()))
    print("{0} has {1} children with names {2}".format(
        get_node_path(zk.get_root_path()), len(children), children))
except:
    print('No root path')
    exit()

if delete_only:
    try:
        zk.remove(get_node_path(zk.get_root_path()))
    except:
        print('Error removing: {0}'.format(get_node_path(zk.get_root_path())))
    zk.disconnect()
    exit()

top_level_config = Configuration(name="top_level",
                                 fetch_on_create=False)
top_level_path = "{0}/{1}".format(get_node_path(zk.get_root_path()),
                                  top_level_config.name)
zk.register(top_level_path, top_level_config)

child1_config = Configuration(name="child1",
                              fetch_on_create=False,
                              data={"str_attr": "str_value",
                                    "int_attr": 1})

child1_path = "{0}/{1}/{2}".format(get_node_path(zk.get_root_path()),
                                   top_level_config.name,
                                   child1_config.name)
zk.register(child1_path, child1_config)

child1_config_fetched = zk.fetch(child1_path)
for key, value in child1_config_fetched.items():
    assert child1_config_fetched[key] == child1_config[key]

zk.disconnect()
