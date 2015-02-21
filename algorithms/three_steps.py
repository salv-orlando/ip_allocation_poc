import datetime
import random
import time

import netaddr
from oslo_db import exception as db_exc
from sqlalchemy import desc

import constants
import db
import log
import test_exceptions as ipam_exc

REQUESTED = 'REQUESTED'
ALLOCATED = 'ALLOCATED'
RECYCLABLE = 'RECYCLABLE'
STATUSES = ['REQUESTED', 'ALLOCATED', 'RECYCLABLE']


def run(*args, **kwargs):
    kwargs['random_all'] = False
    _run(*args, **kwargs)


def run_rnd(*args, **kwargs):
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
    attempt = 0
    while attempt < constants.MAX_ATTEMPTS:
        unique_ts = None
        unique_id = None
        seq_no = None
        # Phase 1 - Grab IP address (if not specified)
        subnet = session.query(db.Subnet).filter_by(
            id=subnet_id).one()
        if ip_address:
            db._verify_ip(session, subnet_id, ip_address)
        elif random_all:
            (ip_address, all_pool_id,
             unique_ts, unique_id) = find_random_ip_address(
                session, subnet)
        else:
            (ip_address, all_pool_id, seq_no,
             unique_ts, unique_id) = find_ip_address(
                session, subnet)
        thread_log.info("Selected IP address:%s", ip_address,
                        event="ip_select")
        # Phase 2 - Create IP request
        if unique_ts:
            recycle_ip_request(session, subnet_id,
                               all_pool_id, ip_address, seq_no)
        else:
            unique_ts, unique_id = store_ip_request(
                session, subnet_id, all_pool_id,
                ip_address, seq_no)
        thread_log.info("Stored request for IP:%s with timestamp:%d",
                        ip_address, unique_ts, event='ip_request_store')
        # Phase 3 - Confirm request and solve contention
        try:
            confirm_ip_request(session, subnet_id, all_pool_id,
                               ip_address, unique_ts, unique_id, thread_log)
            thread_log.info("Attempt:%d succeded, IP:%s allocated",
                            attempt, ip_address,
                            event='attempt%d-success' % attempt)
            break
        except ipam_exc.IpAddressGenerationFailure:
            # Delay status update set to let the other steps of the algorithm
            # do concurrent operations on the same record. This should avoid
            # deadlock issues
            time.sleep(0.05)
            _set_recyclable(session, unique_id)
            thread_log.info("Attempt:%d failed, retrying", attempt,
                            event='attempt%d-fail' % attempt)
            ip_address = None
            attempt = attempt + 1


def find_ip_address(session, subnet):
    for all_pool in subnet.allocation_pools:
        ip_request_query = session.query(db.IPRequest).filter_by(
            allocation_pool_id=all_pool.id).order_by(
            desc(db.IPRequest.seq_no))
        last_item = ip_request_query.first()
        ip_range = netaddr.IPRange(all_pool['first_ip'],
                                   all_pool['last_ip'])
        if last_item:
            seq_no = last_item['seq_no'] or 0
            if seq_no + 1 < ip_range.size:
                return (ip_range[seq_no + 1],
                        all_pool['id'],
                        seq_no + 1,
                        None, None)
        else:
            # first IP works for us
            return ip_range[0], all_pool['id'], 0, None, None
        # Try recycled ip addresses
        ip_request_query = session.query(db.IPRequest).filter_by(
            allocation_pool=all_pool.id, status=RECYCLABLE).order_by(
            db.IPRequest.seq_no)
        ip_address = ip_request_query.first()
        if ip_address:
            return (ip_address['ip_address'],
                    ip_address['all_pool_id'],
                    ip_address['seq_no'],
                    ip_address['timestamp'],
                    ip_address['unique_id'])
    # No address so sorry...
    raise ipam_exc.IpAddressGenerationFailure(
        subnet_id=subnet['id'])


def find_random_ip_address(session, subnet):
    random.seed()
    now = datetime.datetime.utcnow()
    for all_pool in subnet.allocation_pools:
        ip_range = netaddr.IPRange(all_pool['first_ip'],
                                   all_pool['last_ip'])
        ip_address = ip_range[random.randint(0, ip_range.size - 1)]
        ip_request_query = session.query(db.IPRequest).filter_by(
            allocation_pool_id=all_pool.id,
            ip_address=ip_address)
        last_item = ip_request_query.first()
        if last_item:
            # Address is still good if recyclable
            if (last_item['status'] == RECYCLABLE or
                last_item['expiration'] < now):
                return (last_item['ip_address'],
                        last_item['allocation_pool_id'],
                        last_item['timestamp'],
                        last_item['unique_id'])
        else:
            # Address is still available in the pool
            return ip_address, all_pool['id'], None, None

    # No address so sorry...
    raise ipam_exc.IpAddressGenerationFailure(
        subnet_id=subnet['id'])


def store_ip_request(session, subnet_id, all_pool_id, ip_address, seq_no=None):
    timestamp = db.set_timestamp()
    unique_id = db.generate_uuid()
    with session.begin():
        ip_request = db.IPRequest(
            subnet_id=subnet_id,
            allocation_pool_id=all_pool_id,
            seq_no=seq_no,
            ip_address=ip_address,
            timestamp=timestamp,
            unique_id=unique_id,
            expiration=(datetime.datetime.now() +
                        datetime.timedelta(
                            0, constants.RESV_EXPIRATION)),
            status=REQUESTED)
        session.add(ip_request)
    return timestamp, unique_id


def recycle_ip_request(session, subnet_id, all_pool_id, ip_address,
                       seq_no=None):
    now = datetime.datetime.utcnow()
    with session.begin():
        ip_request_query = session.query(db.IPRequest).filter(
            db.IPRequest.expiration < now).filter_by(
            subnet_id=subnet_id,
            allocation_pool_id=all_pool_id,
            ip_address=ip_address,
            status=RECYCLABLE)
        ip_request_query.update(
            {'status': REQUESTED,
             'expiration': now + datetime.timedelta(0, 60)})


def _set_allocated(session, exp_time, subnet_id, unique_id, req_timestamp):
        update_ip_request_query = session.query(db.IPRequest).filter(
            db.IPRequest.expiration < exp_time).filter_by(
            unique_id=unique_id,
            timestamp=req_timestamp)
        upd_count = update_ip_request_query.update({'status': ALLOCATED},
                                                   synchronize_session=False)
        if upd_count != 1:
            raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)


def _set_recyclable(session, unique_id):
        update_ip_request_query = session.query(db.IPRequest).filter_by(
            unique_id=unique_id)
        update_ip_request_query.update({'status': RECYCLABLE},
                                       synchronize_session=False)


def confirm_ip_request(session, subnet_id, all_pool_id, ip_address,
                       req_timestamp, unique_id, thread_log):
    now = datetime.datetime.utcnow()
    with session.begin():
        ip_request_query = session.query(db.IPRequest).filter(
            db.IPRequest.expiration < now).filter_by(
            allocation_pool_id=all_pool_id,
            ip_address=ip_address)
        ip_requests = []
        for ip_request in ip_request_query:
            if ip_request['status'] == ALLOCATED:
                thread_log.info("IP address %s already allocated by %d, adieu",
                                ip_address, ip_request['timestamp'])
                raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)
            ip_requests.append(ip_request)
        # if we are here there is contention between ip requests
        ip_requests = sorted(ip_requests, key=lambda x: x['timestamp'])
        if ip_requests[0]['timestamp'] != req_timestamp:
            # another concurrent request came earlier
            thread_log.info("Address %s - priority for %d, giving up",
                            ip_address, ip_requests[0]['timestamp'])
            raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)
        # attempt to update current IP request to allocated
        thread_log.info("Attempting to mark %s as allocated", ip_address)
        _set_allocated(session, now, subnet_id, unique_id, req_timestamp)
        thread_log.info("Marked %s as allocated", ip_address)
    with session.begin():
        # update timestamps for all other concurrent requests
        ip_request_query = session.query(db.IPRequest).filter_by(
            allocation_pool_id=all_pool_id,
            ip_address=ip_address)
        req_timestamps = [ip_request['timestamp']
                          for ip_request in ip_request_query]
        if req_timestamp not in req_timestamps:
            thread_log.info("Ahhh somebody screw me up and took %s",
                            ip_address)
            raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)
        else:
            req_timestamps.remove(req_timestamp)
        thread_log.info("There are %d request timestamps to update",
                        len(req_timestamps))
        if req_timestamps:
            update_timestamp_req_query = session.query(db.IPRequest).filter(
                db.IPRequest.timestamp.in_(req_timestamps))
            upd_count = update_timestamp_req_query.update(
                {'timestamp': req_timestamp}, synchronize_session=False)
            thread_log.info("Updated %d timestamps, expected:%d", upd_count,
                            len(req_timestamps))
            if upd_count != len(req_timestamps):
                thread_log.info("Concurrent timestamp update for request %d "
                                "for IP %s - exp:%d, actual:%d",
                                req_timestamp, ip_address,
                                len(req_timestamps), upd_count)
                raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)


def verify_correctness(session, subnet_id):
    recyclable_ips = db.get_ip_requests(session, subnet_id, RECYCLABLE)
    requested_ips = db.get_ip_requests(session, subnet_id, REQUESTED)
    allocated_ips = db.get_ip_requests(session, subnet_id, ALLOCATED)
    print("The process left behind %d RECYCLABLE IPs", len(recyclable_ips))
    if len(requested_ips):
        print("Found %d ip requests in status REQUESTED. There should be none",
              len(requested_ips))
        return False
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
