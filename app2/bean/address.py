class Address(object):
    def __init__(self, address):
        self.ip=str(address.ip)
        self.hostname=str(address.hostname)
        self.port=str(address.port)