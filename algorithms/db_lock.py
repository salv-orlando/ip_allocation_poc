import time

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
    thread_logs.append(thread_log)
    session = db.get_session(sql_connection)
    ip_address = allocate_ip(
        session, thread_log, subnet_id, ip_address,
        sleep_time=steps.get(
            'request', constants.DEFAULT_REQUEST_TIME) / 1000.0)
    thread_log.info("IP Request committed", event='ip_request')


def allocate_ip(session, logger, subnet_id, ip_address=None, sleep_time=0):
    # sleep before creating ip request
    time.sleep(sleep_time)
    try:
        with session.begin():
            if ip_address:
                db._verify_ip(session, subnet_id, ip_address)
            else:
                ip_address = db._locking_generate_ip(
                    session, subnet_id, logger)
            ip_request = db.IPRequest(
                subnet_id=subnet_id,
                ip_address=ip_address,
                status='ALLOCATED')
            session.add(ip_request)
    except db_exc.DBDuplicateEntry as dup_exc:
        logger.info("Abort at phase 1 - attempt %d", attempt,
                    transaction_abort='p1-attempt-%d' % attempt)
        raise
    return ip_address
