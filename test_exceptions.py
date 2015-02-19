class NeutronException(Exception):
    """Base Neutron Exception.

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """
    message = "An unknown exception occurred."

    def __init__(self, **kwargs):
        super(NeutronException, self).__init__(self.message % kwargs)
        self.msg = self.message % kwargs

    def __unicode__(self):
        return unicode(self.msg)

    def use_fatal_exceptions(self):
        return False


class IpAddressAlreadyAllocated(NeutronException):
    message = ("IP address %(ip)s already allocated in subnet %(subnet_id)s")


class InvalidIpForSubnet(NeutronException):
    message = ("IP address %(ip)s does not belong to subnet %(subnet_id)s")


class InvalidAddressRequest(NeutronException):
    message = ("The address allocation request could not be satisfied "
               "because: %(reason)s")


class AllocationOnAutoAddressSubnet(NeutronException):
    message = (("IPv6 address %(ip)s cannot be directly "
                "assigned to a port on subnet %(subnet_id)s as the "
                "subnet is configured for automatic addresses"))


class IpAddressGenerationFailure(NeutronException):
    message = ("No more IP addresses available for subnet %(subnet_id)s.")
