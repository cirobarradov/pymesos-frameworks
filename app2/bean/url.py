from address import Address

class Url(object):
    def __init__(self, url):
        self.path=str(url.path)
        self.scheme=str(url.scheme)
        self.address=Address(url.address)
