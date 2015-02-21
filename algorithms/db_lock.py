import time

from oslo_db import exception as db_exc

import constants
import db
import log

""" Locking IP allocation algorithm

This algorithm simply locks the availability ranges for a subnet until the
IP allocation process is complete. It does so leveraging SELECT...FOR UPDATE
queries.

In single master mode this simply means IP allocation is a serial process.
In multi master mode every the database lock is enforced only at node level.
Therefore IP allocation might happen concurrently on different replicas.
This situation will lead to conflicts which are signalled by SqlAlchemy as
database deadlock errors.
When this error occurr, the transaction should be retried. Methods such as
exponential backoff can signfigicantly reduce the chance of conflict.

"""

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
    attempt = 0
    while attempt < constants.MAX_ATTEMPTS:
        time.sleep(sleep_time)
        try:
            logger.info("Starting attempt: %d", attempt,
                        transaction_start='attempt-%d' % attempt)
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
                logger.info("Marking %s as ALLOCATED", ip_address)
        except db_exc.DBDuplicateEntry as dup_exc:
            logger.info("Abort - attempt %d", attempt,
                        transaction_abort='attempt-%d' % attempt)
            raise
        except db_exc.DBDeadlock as deadlock_exc:
            # TODO: exponential backoff
            logger.info("Abort - attempt %d", attempt,
                        transaction_abort='attempt-%d' % attempt)
            logger.warning("Transaction aborted because of deadlock "
                           "error, retrying")
            ip_address = None
            attempt = attempt + 1
        # If we are here the operation was successful
        logger.info("Commit - attempt %d", attempt,
                    transaction_commit='attempt-%d' % attempt)
        break
    return ip_address


def verify_correctness(session, subnet_id):
    allocated_ips = db.get_ip_requests(session, subnet_id, 'ALLOCATED')
    print("")
    print("Allocated IP Addresses:")
    print("-----------------------")
    for ip_address in allocated_ips:
        print ip_address
    print("")
    len_diff = len(allocated_ips) - len(set(allocated_ips))
    if len_diff:
        print("There are %d duplicates in allocated IPs."
              "This is bad" % len_diff)
        return False
    return True
