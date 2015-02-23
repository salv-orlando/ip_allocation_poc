import datetime
import time

import netaddr
from oslo_db import exception as db_exc

import constants
import db
import log

""" IP Allocation in two steps with retries.

This algorith uses the same data structutes as the one based on locking
database queries. There is a table for storing IP Requests, where
(ip_address, subnet_id) is the primary key and a table for keeping track
of availability ranges.

However, this algorithm does not lock the availability ranges while the
updates are carried out. This means that primary key and unique indexes
violations are tolerated and dealt with both while creating the IP
request and adjusting availability ranges.
As multiple IP requests on the same subnet can be concurently committed,
the process for adjusting available range must be aware of this condition.
The routine presented takes into account the fact that multiple IP address
might be ready to be removed from IP avaiability ranges at a given time,
and that some other thread or process might have already taken care of the
IP address the routine is trying to adjust availability ranges for.
In a way, the solution adopted by this algorithm to adjust availability
ranges is idempotent.

The algorithm is presented in four flavours:
    - sequential allocation, range check
    Perform sequential allocation and check whether the IP has been removed
    from IP availability ranges before retrying
    - sequential allocation, no range check
    Perform sequential allocation, retry without checking if some other process
    removed the IP from the ranges
    - random allocation, range check
    Instead of performing sequential allocation, pick a random address from pools
    - random allocation, no range check
    Pick random allocation from pools, and then behave as the corresponding
    sequential algorithm
"""

def run_with_range_check(*args, **kwargs):
    kwargs['verify_ranges'] = True
    _run(*args, **kwargs)


def run_without_range_check(*args, **kwargs):
    kwargs['verify_ranges'] = False
    _run(*args, **kwargs)


def run_rnd_with_range_check(*args, **kwargs):
    kwargs['verify_ranges'] = True
    kwargs['random_all'] = True
    _run(*args, **kwargs)


def run_rnd_without_range_check(*args, **kwargs):
    kwargs['verify_ranges'] = False
    kwargs['random_all'] = True
    _run(*args, **kwargs)


def _run(*args, **kwargs):
    sql_connection = args[0]
    t_name = kwargs.get('name')
    thread_log = log.getLogger(t_name)
    steps = kwargs.get('steps')
    thread_logs = kwargs['thread_logs']
    subnet_id = kwargs['subnet_id']
    ip_address = kwargs.get('ip_address')
    verify_ranges = kwargs.get('verify_ranges', True)
    random_all = kwargs.get('random_all', False)
    thread_logs.append(thread_log)
    session = db.get_session(sql_connection)
    ip_address, attempts_1 = allocate_ip(
        session, thread_log, subnet_id, ip_address,
        random_all=random_all, sleep_time=steps.get(
            'request', constants.DEFAULT_REQUEST_TIME) / 1000.0)
    thread_log.info("IP Request committed", event='ip_request')

    attempts_2 = adjust_av_ranges(
        session, thread_log, subnet_id, ip_address,
        check_ranges_before_retry=verify_ranges,
        sleep_time=steps.get(
            'av_ranges', constants.DEFAULT_REQUEST_TIME) / 1000.0)
    thread_log.attempts = attempts_1 + attempts_2


def allocate_ip(session, logger, subnet_id, ip_address=None, sleep_time=0,
                random_all=False):
    attempt = 0
    if random_all:
        generate_func = db._generate_random_ip
    else:
        generate_func = db._generate_ip
    # STEP 1: IP address allocation
    # This step is fairly simple: try to acquire an IP address from an
    # availability range. If then the procedure fails because of a duplicated
    # ip address, then try extracting another one from availability ranges
    while attempt < constants.MAX_ATTEMPTS:
        # sleep before creating ip request
        time.sleep(sleep_time)
        try:
            logger.info("Phase 1 - Attempt: %d", attempt,
                        transaction_start='p1-attempt-%d' % attempt)
            with session.begin():
                if ip_address:
                    db._verify_ip(session, subnet_id, ip_address)
                else:
                    ip_request_query = session.query(db.IPRequest).filter_by(
                        status='RESERVED', subnet_id=subnet_id)
                    ip_request_query = ip_request_query.filter(
                        datetime.datetime.now() < db.IPRequest.expiration)
                    ip_address = generate_func(
                        session, subnet_id, logger,
                        skip_ips=[item['ip_address'] for item
                                  in ip_request_query])
                logger.info("Phase 1 - Attempt:%d, IP:%s", attempt, ip_address,
                            event='phase1-attempt-%d' % attempt)
                ip_request = db.IPRequest(
                    subnet_id=subnet_id,
                    ip_address=ip_address,
                    expiration=(datetime.datetime.now() +
                                datetime.timedelta(
                                    0, constants.RESV_EXPIRATION)),
                    status='RESERVED')
                session.add(ip_request)
        except db_exc.DBDuplicateEntry as dup_exc:
            logger.info("Abort at phase 1 - attempt %d", attempt,
                        transaction_abort='p1-attempt-%d' % attempt)
            attempt = attempt + 1
            if attempt == constants.MAX_ATTEMPTS:
                logger.info("Maximum number of attempts exceeded",
                            event='timeout')
                raise
            logger.debug("IP address %s allocation failed,"
                         "retrying", ip_address)
            ip_address = None
            continue
        logger.info("Phase 1 completed for IP:%s", ip_address,
                    event='phase1-end',
                    transaction_commit='p1-attempt-%d' % attempt)
        return ip_address, attempt


def _check_ip_address_in_ranges(session, ip_address, subnet_id, logger):
    range_qry = session.query(
        db.IPAvailabilityRange).join(
        db.IPAllocationPool).filter_by(
        subnet_id=subnet_id)
    ip_set = netaddr.IPSet()
    for db_range in range_qry:
        logger.warning("DB RANGE:%s", db_range)
        ip_set.add(netaddr.IPRange(db_range['first_ip'],
                                   db_range['last_ip']))
    return not netaddr.IPAddress(ip_address) in ip_set


def adjust_av_ranges(session, logger, subnet_id, ip_address=None,
                     check_ranges_before_retry=True, sleep_time=0):
    # STEP 2: Adjust availability ranges
    # Do this operation in an idempotent fashion: try to allocate all IPs which
    # are in "reserved" status, and mark them as "allocated" while committing
    # the transaction. If the commit fails for a duplicated entry error, this
    # implies that some other operation concurrently updated availability
    # ranges. In this case the algorithm checks whether the concurrent update
    # also sorted the ip address this transaction is allocating, otherwise
    # retries. If the maximum attempt of retries is exceed the reserved
    # allocation is removed.
    attempt = 0
    while attempt < constants.MAX_ATTEMPTS:
        # sleep before adjusting availability ranges
        time.sleep(sleep_time)
        try:
            logger.info("Phase 2 - Attempt: %d", attempt,
                        transaction_start='p2-attempt-%d' % attempt)
            with session.begin():
                ip_request_query = session.query(db.IPRequest)
                ip_request_query = ip_request_query.filter_by(
                    status='RESERVED', subnet_id=subnet_id)
                ip_request_query = ip_request_query.filter(
                    datetime.datetime.now() < db.IPRequest.expiration)
                # Optimistically set all entries to "ALLOCATED"
                ip_addresses = []
                for item in ip_request_query:
                    ip_addresses.append(item['ip_address'])
                    item['status'] = 'ALLOCATED'
                logger.info("Phase 2 - Attempt:%d, IPs:%s",
                            attempt, ",".join(ip_addresses),
                            event='phase2-attempt-%d' % attempt)
                db._allocate_specific_ips(session, subnet_id,
                                          ip_addresses, logger)
        except db_exc.DBDuplicateEntry as dup_exc:
            logger.info("Abort at phase 2 - attempt %d", attempt,
                        transaction_abort='p2-attempt-%d' % attempt)
            if check_ranges_before_retry:
                # Do not retry if the IP address was removed from availability
                # ranges in some other transaction
                if _check_ip_address_in_ranges(session, ip_address, subnet_id,
                                               logger):
                    logger.info("IP address %s not anymore in availability "
                                "ranges. Skipping resize", ip_address,
                                event='concurrent_update')
                    return attempt
            attempt = attempt + 1
            if attempt == constants.MAX_ATTEMPTS:
                logger.info("Maximum number of attempts exceeded",
                            event='timeout')
                # delete IP request entry
                ip_request_qry = session.query(db.IPRequest).filter_by(
                    subnet_id=subnet_id, ip_address=ip_address).delete()
                raise

            logger.debug("Failed ajusting IP av ranges for: %s "
                         "retrying", ",".join(ip_addresses))
            continue
        logger.info("Phase 2 completed for IP:%s", ip_address,
                    event='phase2-end',
                    transaction_commit='p2-attempt-%d' % attempt)
        return attempt


def verify_correctness(session, subnet_id):
    # TODO: everything
    return True
