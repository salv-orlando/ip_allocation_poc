import datetime
import time

import netaddr
from oslo_db import exception as db_exc

import constants
import db
import log

def run(*args, **kwargs):
    sql_connection = args[0]
    t_name = kwargs.get('name')
    thread_log = log.getLogger(t_name)
    steps = kwargs.get('steps')
    thread_logs = kwargs['thread_logs']
    subnet_id = kwargs['subnet_id']
    ip_address = kwargs.get('ip_address')
    verify_ranges = kwargs.get('verify_ranges', True)
    random_all = kwargs.get('random_all', False)



