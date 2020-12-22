

class Link(object):

    bandwidth = 1

    def __init__(self, node1, node2, weight=None):
        self.node1 = node1
        self.node2 = node2
        self.weight = weight

    def transmit(self, message):
        return self.bandwidth
