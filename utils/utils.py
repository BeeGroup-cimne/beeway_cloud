import importlib
import json
import pkgutil
import logging
logging.basicConfig(level=logging.INFO)


def read_config(conf_file):
    with open(conf_file) as config_f:
        config = json.load(config_f)
    return config


def log_string(text):
    logging.info(text)
