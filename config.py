import copy
import json

_config = None
default_config_path = './confin.json'


def get_config(path=default_config_path):
    global _config
    if _config is None:
        with open(path, 'r') as fh:
            _config = json.load(fh)
    return copy.deepcopy(_config)
