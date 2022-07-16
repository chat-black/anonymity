import json
import socket
from typing import Set


def get_all_ip() -> Set[str]:
    addr = socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET)
    res = set()
    for item in addr:
        res.add(item[-1][0])
    res.add('127.0.0.1')
    return res


def get_username():
    with open('config.json', 'r') as fin:
        content = fin.read().strip()
    content = json.loads(content)
    if 'username' in content and 'value' in content['username']:
        return content['username']['value']
    return 'username'


def get_dbname():
    with open('config.json', 'r') as fin:
        content = fin.read().strip()
    content = json.loads(content)
    if 'dbname' in content and 'value' in content['dbname']:
        return content['dbname']['value']
    return 'tpch'
