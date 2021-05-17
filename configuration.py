import configparser
import logging
import sys
import time

import keyring


def setup_logger(name, log_file, level=logging.INFO):
    """To setup as many loggers as you want"""

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())

    return logger


config = configparser.ConfigParser()
config.read('config.ini')

logger = setup_logger(f"default", f"{config['Application']['log_path']}")
error_logger = setup_logger('error_logger', f"{config['Application']['log_path']}", level=logging.ERROR)

tm1_connection = config['Tm1']
tm1_connection['password'] = keyring.get_password("system", tm1_connection['user'])

app_name = config['Application']['app_name']
max_workers = int(config['Application']['max_workers'])
max_attempts = int(config['Application']['max_attempts'])
chunk_size = int(config['Application']['chunk_size'])
target = config['Application']['target']

t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t)


