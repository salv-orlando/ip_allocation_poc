import datetime
import random
import time
import uuid

import netaddr
from oslo_db.sqlalchemy import models
from oslo_db.sqlalchemy import session
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy.ext import declarative
from sqlalchemy import orm
from sqlalchemy.orm import exc as orm_exc
import sqlalchemy as sa

import log
import test_exceptions as ipam_exc

DHCPV6_STATEFUL = 'dhcpv6-stateful'
DHCPV6_STATELESS = 'dhcpv6-stateless'
IPV6_SLAAC = 'slaac'

_FACADE = None
logger = log.getLogger('db_profiler')
query_stats = {}


def set_query_stats(statement, query_time):

    def do_verb(verb):
        if verb in statement:
            query_stats.setdefault(verb, []).append(query_time)

    do_verb('SELECT')
    do_verb('INSERT')
    do_verb('DELETE')
    do_verb('UPDATE')


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                          parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start Query: %s" % statement)


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                         parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    set_query_stats(statement, total)
    logger.info("Total Time: %.5f" % total)


def _create_facade_lazily(sql_connection):
    global _FACADE

    if _FACADE is None:
        _FACADE = session.EngineFacade(
            sql_connection, slave_connection=None,
            sqlite_fk=True)

    return _FACADE


def get_engine(sql_connection):
    """Helper method to grab engine."""
    facade = _create_facade_lazily(sql_connection)
    return facade.get_engine()


def get_session(sql_connection, autocommit=True, expire_on_commit=False):
    """Helper method to grab session."""
    facade = _create_facade_lazily(sql_connection)
    return facade.get_session(autocommit=autocommit,
                              expire_on_commit=expire_on_commit)


class ModelBase(models.ModelBase):
    """Base class for Neutron Models."""

    __table_args__ = {'mysql_engine': 'InnoDB'}

    def __iter__(self):
        self._i = iter(orm.object_mapper(self).columns)
        return self

    def next(self):
        n = self._i.next().name
        return n, getattr(self, n)

    def __repr__(self):
        """sqlalchemy based automatic __repr__ method."""
        items = ['%s=%r' % (col.name, getattr(self, col.name))
                 for col in self.__table__.columns]
        return "<%s.%s[object at %x] {%s}>" % (self.__class__.__module__,
                                               self.__class__.__name__,
                                               id(self), ', '.join(items))


BASE = declarative.declarative_base(cls=ModelBase)


class IPRequest_std(BASE):
    __tablename__ = 'iprequest_1'
    ip_address = sa.Column(sa.String(64), nullable=False, primary_key=True)
    subnet_id = sa.Column(sa.String(36), nullable=False, primary_key=True)
    expiration = sa.Column(sa.DateTime(), primary_key=True,
                           default=(datetime.datetime.utcnow() +
                                    datetime.timedelta(0, 60)))
    status = sa.Column(sa.String(36))


def generate_uuid():
    return str(uuid.uuid4())


def set_timestamp():
    return int(time.clock() * 1000000000)


class IPRequest_alt(BASE):
    __tablename__ = 'iprequest_2'
    ip_address = sa.Column(sa.String(64), nullable=False)
    unique_id = sa.Column(sa.String(36),
                          primary_key=True,
                          index=True,
                          default=generate_uuid)
    subnet_id = sa.Column(sa.String(36), nullable=False, index=True)
    status = sa.Column(sa.String(36), index=True)
    timestamp = sa.Column(sa.BigInteger(), index=True,
                          default=set_timestamp())
    expiration = sa.Column(sa.DateTime(),
                           default=(datetime.datetime.utcnow() +
                                    datetime.timedelta(0, 60)))
    seq_no = sa.Column(sa.Integer(), nullable=True)
    allocation_pool_id = sa.Column(sa.String(36),
                                   sa.ForeignKey('ipallocationpools.id',
                                                 ondelete="CASCADE"),
                                   nullable=False, index=True)
    addr_pool_ip_idx = sa.Index('addr_pool_ip_idx',
                                ip_address,
                                allocation_pool_id)

IPRequest = IPRequest_std


def set_ip_request_model(algorithm):
    global IPRequest
    if algorithm.startswith('3-step'):
        IPRequest = IPRequest_alt


class HasId(object):
    """id mixin, add to subclasses that have an id."""

    id = sa.Column(sa.String(36),
                   primary_key=True,
                   default=str(uuid.uuid4()))


class IPAvailabilityRange_std(BASE):
    """Internal representation of available IPs for Neutron subnets.

    Allocation - first entry from the range will be allocated.
    If the first entry is equal to the last entry then this row
    will be deleted.
    Recycling ips involves reading the IPAllocationPool and IPAllocation tables
    and inserting ranges representing available ips.  This happens after the
    final allocation is pulled from this table and a new ip allocation is
    requested.  Any contiguous ranges of available ips will be inserted as a
    single range.
    """

    __tablename__ = 'ipavailabilityranges'

    allocation_pool_id = sa.Column(sa.String(36),
                                   sa.ForeignKey('ipallocationpools.id',
                                                 ondelete="CASCADE"),
                                   nullable=False,
                                   primary_key=True)
    first_ip = sa.Column(sa.String(64), nullable=False, primary_key=True)
    last_ip = sa.Column(sa.String(64), nullable=False, primary_key=True)
    __table_args__ = (
        sa.UniqueConstraint(
            first_ip, allocation_pool_id,
            name='uniq_ipavailabilityranges0first_ip0allocation_pool_id'),
        sa.UniqueConstraint(
            last_ip, allocation_pool_id,
            name='uniq_ipavailabilityranges0last_ip0allocation_pool_id'),
        BASE.__table_args__
    )

    def __repr__(self):
        return "%s - %s" % (self.first_ip, self.last_ip)


class IPAvailabilityRange_alt(BASE):

    __tablename__ = 'ipavailabilityranges2'

    allocation_pool_id = sa.Column(sa.String(36),
                                   sa.ForeignKey('ipallocationpools.id',
                                                 ondelete="CASCADE"),
                                   nullable=False,
                                   primary_key=True)
    unique_id = sa.Column(sa.String(36),
                          primary_key=True,
                          default=generate_uuid)
    first_ip = sa.Column(sa.String(64), nullable=False)
    last_ip = sa.Column(sa.String(64), nullable=False)
    __table_args__ = (
        sa.UniqueConstraint(
            first_ip, allocation_pool_id,
            name='uniq_ipavailabilityranges0first_ip0allocation_pool_id'),
        sa.UniqueConstraint(
            last_ip, allocation_pool_id,
            name='uniq_ipavailabilityranges0last_ip0allocation_pool_id'),
        BASE.__table_args__
    )

    def __repr__(self):
        return "%s - %s" % (self.first_ip, self.last_ip)


IPAvailabilityRange = IPAvailabilityRange_alt


def set_av_range_model(algorithm):
    global IPAvailabilityRange
    if algorithm == 'lock-for-update':
        IPAvailabilityRange = IPAvailabilityRange_std
        IPAllocationPool.available_ranges = orm.relationship(
            IPAvailabilityRange,
            backref='ipallocationpool',
            lazy="joined",
            cascade='all, delete-orphan')


class IPAllocationPool(BASE, HasId):
    """Representation of an allocation pool in a Neutron subnet."""

    __tablename__ = 'ipallocationpools'

    subnet_id = sa.Column(sa.String(36), sa.ForeignKey('subnets.id',
                                                       ondelete="CASCADE"),
                          nullable=True)
    first_ip = sa.Column(sa.String(64), nullable=False)
    last_ip = sa.Column(sa.String(64), nullable=False)
    available_ranges = orm.relationship(IPAvailabilityRange,
                                        backref='ipallocationpool',
                                        lazy="joined",
                                        cascade='all, delete-orphan')

    def __repr__(self):
        return "%s - %s" % (self.first_ip, self.last_ip)


class Subnet(BASE, HasId):
    """Represents a neutron subnet.

    When a subnet is created the first and last entries will be created. These
    are used for the IP allocation.
    """

    __tablename__ = 'subnets'

    name = sa.Column(sa.String(255))
    ip_version = sa.Column(sa.Integer, nullable=False)
    cidr = sa.Column(sa.String(64), nullable=False)
    gateway_ip = sa.Column(sa.String(64))
    allocation_pools = orm.relationship(IPAllocationPool,
                                        backref='subnet',
                                        lazy="joined",
                                        cascade='delete')
    enable_dhcp = sa.Column(sa.Boolean())
    shared = sa.Column(sa.Boolean)
    ipv6_ra_mode = sa.Column(sa.Enum(IPV6_SLAAC,
                                     DHCPV6_STATEFUL,
                                     DHCPV6_STATELESS,
                                     name='ipv6_ra_modes'), nullable=True)
    ipv6_address_mode = sa.Column(sa.Enum(IPV6_SLAAC,
                                  DHCPV6_STATEFUL,
                                  DHCPV6_STATELESS,
                                  name='ipv6_address_modes'), nullable=True)

# Operations


def create_subnet_alt(session, subnet_id, cidr, ip_version,
                      allocation_pools, name="meh"):
    with session.begin(subtransactions=True):
        # The 'shared' attribute for subnets is for internal plugin
        # use only. It is not exposed through the API
        args = {'id': subnet_id,
                'name': name,
                'ip_version': ip_version,
                'cidr': cidr,
                'enable_dhcp': False,
                'shared': False}
        subnet = Subnet(**args)

        session.add(subnet)

        for pool in allocation_pools:
            ip_pool = IPAllocationPool(subnet=subnet,
                                       first_ip=pool['start'],
                                       last_ip=pool['end'])
            session.add(ip_pool)
            ip_range = IPAvailabilityRange(
                ipallocationpool=ip_pool,
                first_ip=pool['start'],
                last_ip=pool['end'],
                unique_id=str(uuid.uuid4))
            session.add(ip_range)
    return subnet


def create_subnet(session, subnet_id, cidr, ip_version,
                  allocation_pools, name="meh"):
    with session.begin(subtransactions=True):
        # The 'shared' attribute for subnets is for internal plugin
        # use only. It is not exposed through the API
        args = {'id': subnet_id,
                'name': name,
                'ip_version': ip_version,
                'cidr': cidr,
                'enable_dhcp': False,
                'shared': False}
        subnet = Subnet(**args)

        session.add(subnet)

        for pool in allocation_pools:
            ip_pool = IPAllocationPool(subnet=subnet,
                                       first_ip=pool['start'],
                                       last_ip=pool['end'])
            session.add(ip_pool)
            ip_range = IPAvailabilityRange(
                ipallocationpool=ip_pool,
                first_ip=pool['start'],
                last_ip=pool['end'])
            session.add(ip_range)
    return subnet


def _check_unique_ip(session, subnet_id, ip_address):
    """Validate that the IP address on the subnet is not in use."""
    ip_qry = session.query(IPRequest)
    try:
        ip_qry.filter_by(subnet_id=subnet_id,
                         ip_address=ip_address).one()
    except orm_exc.NoResultFound:
        return True
    return False


def _verify_ip(session, subnet_id, ip_address):
    """Verify whether IP address can be allocated on subnet.

    :param session: database session
    :param ip_address: String representing the IP address to verify
    :raises: InvalidInput, IpAddressAlreadyAllocated
    """
    # Ensure that the IP's are unique
    if not _check_unique_ip(session, subnet_id, ip_address):
        raise ipam_exc.IpAddressAlreadyAllocated(
            subnet_id=subnet_id,
            ip=ip_address)


def _verify_ip_alt(session, subnet_id, ip_address):
    ip_qry = session.query(IPRequest)
    now = datetime.datetime.utcnow()
    try:
        ip_qry.filter_by(
            subnet_id=subnet_id,
            ip_address=ip_address).filter(
            IPRequest.status != 'RECYCLABLE' and IPRequest.expiration > now)
    except orm_exc.NoResultFound:
        return True
    raise ipam_exc.IpAddressAlreadyAllocated(
        subnet_id=subnet_id,
        ip=ip_address)


def _generate_ip(session, subnet_id, logger, skip_ips=[]):
    return _try_generate_ip(session, subnet_id, logger, skip_ips)


def _try_generate_ip(session, subnet_id, logger, skip_ips=[]):
    """Generate an IP address from availability ranges."""
    range_qry = session.query(
        IPAvailabilityRange).join(IPAllocationPool)
    chosen_ip = None
    for ip_range in range_qry.filter_by(subnet_id=subnet_id):
        for ip_address in netaddr.IPRange(
                ip_range['first_ip'], ip_range['last_ip']):
            if str(ip_address) not in skip_ips:
                chosen_ip = str(ip_address)
                logger.debug("Allocated IP - %(ip_address)s from range "
                             "[%(first_ip)s; %(last_ip)s]",
                             {'ip_address': ip_address,
                              'first_ip': ip_address,
                              'last_ip': ip_range['last_ip']})
                break
    if not chosen_ip:
        logger.debug("All IPs from subnet %s allocated", subnet_id)
        raise ipam_exc.IpAddressGenerationFailure(
            subnet_id=subnet_id)
    return chosen_ip


def _generate_random_ip(session, subnet_id, logger, skip_ips=[]):
    """Generate a random IP address from availability ranges."""
    range_qry = session.query(
        IPAvailabilityRange).join(IPAllocationPool)
    ip_range = range_qry.filter_by(subnet_id=subnet_id).first()
    if not ip_range:
        logger.debug("All IPs from subnet %s allocated", subnet_id)
        raise ipam_exc.IpAddressGenerationFailure(
            subnet_id=subnet_id)
    range_obj = netaddr.IPRange(
        ip_range['first_ip'], ip_range['last_ip'])
    random.seed()
    chosen_ip = range_obj[random.randint(0, len(range_obj) - 1)]

    logger.debug("Allocated IP - %(ip_address)s from range "
                 "[%(first_ip)s; %(last_ip)s]",
                 {'ip_address': chosen_ip,
                  'first_ip': ip_range['first_ip'],
                  'last_ip': ip_range['last_ip']})
    return chosen_ip


def _locking_generate_ip(session, subnet_id, logger):
    range_qry = session.query(
        IPAvailabilityRange).join(IPAllocationPool).with_lockmode('update')
    ip_range = range_qry.filter_by(subnet_id=subnet_id).first()
    if not ip_range:
        logger.debug("All IPs from subnet %s allocated", subnet_id)
        raise ipam_exc.IpAddressGenerationFailure(subnet_id=subnet_id)

    ip_address = ip_range['first_ip']
    if ip_range['first_ip'] == ip_range['last_ip']:
        # No more free indices on subnet => delete
        logger.debug("No more free IP's in slice. Deleting allocation pool.")
        session.delete(ip_range)
    else:
        # increment the first free
        new_first_ip = str(netaddr.IPAddress(ip_address) + 1)
        ip_range['first_ip'] = new_first_ip
        logger.debug("Allocated IP - %(ip_address)s from %(first_ip)s "
                     "to %(last_ip)s",
                     {'ip_address': ip_address,
                      'first_ip': ip_address,
                      'last_ip': ip_range['last_ip']})
    return ip_address


def _allocate_specific_ips(session, subnet_id, ip_addresses, logger):
    logger.debug("Removing %(ip_addresses)s from availability ranges for "
                 "subnet id:%(subnet_id)s",
                 {'ip_addresses': ip_addresses,
                  'subnet_id': subnet_id})
    # NOTE: the Neutron DB IPAM driver can leverage access to the whole
    # Neutron DB - therefore it can perform queries also on models which
    # are not exclusive to the IPAM driver
    range_qry = session.query(
        IPAvailabilityRange).join(
        IPAllocationPool).filter_by(
        subnet_id=subnet_id)
    # Netaddr's IPRange and IPSet objects work very well even with very
    # large subnets, including IPv6 ones.
    final_ranges = []
    for db_range in range_qry:
        initial_ip_set = netaddr.IPSet(netaddr.IPRange(
            db_range['first_ip'], db_range['last_ip']))
        final_ip_set = initial_ip_set - netaddr.IPSet(ip_addresses)
        if initial_ip_set == final_ip_set:
            # No IP address falls within the current range, move to the
            # next one
            final_ranges.append(db_range)
            continue
        # delete initial range
        #session.delete(db_range)
        # flush the session to prevent UNIQUE constraint violations
        for new_range in final_ip_set.iter_ipranges():
            # store new range in database
            # use netaddr.IPAddress format() method which is equivalent
            # to str(...) but also enables us to use different
            # representation formats (if needed) for IPv6.
            first_ip = netaddr.IPAddress(new_range.first)
            last_ip = netaddr.IPAddress(new_range.last)
            if (db_range['first_ip'] == first_ip.format() or
                db_range['last_ip'] == last_ip.format()):
                db_range['first_ip'] = first_ip.format()
                db_range['last_ip'] = last_ip.format()
                logger.warning("Edited DB RANGE:%s", db_range)
                final_ranges.append(db_range)
            else:
                new_ip_range = IPAvailabilityRange(
                    allocation_pool_id=db_range['allocation_pool_id'],
                    first_ip=netaddr.IPAddress(new_range.first).format(),
                    last_ip=netaddr.IPAddress(new_range.last).format())
                session.add(new_ip_range)
                logger.warning("Added new DB RANGE:%s", new_ip_range)
                final_ranges.append(new_ip_range)
    # Most callers might ignore this return value, which is however
    # useful for testing purposes
    logger.debug("Availability ranges for subnet id %(subnet_id)s "
                 "modified: %(new_ranges)s",
                 {'subnet_id': subnet_id,
                  'new_ranges': ", ".join(["[%s; %s]" %
                                           (r['first_ip'], r['last_ip']) for
                                           r in final_ranges])})
    return final_ranges
