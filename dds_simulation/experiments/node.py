from dds_simulation.experiments.dataunit import Dataunit


class Node(object):
    fields = ('identity', 'neighbors')

    def __init__(self, identity, neighbors=None):
        self.identity = identity
        self.neighbors = neighbors

    def add_dataunits(self, dataunits):
        self.dataunits = dataunits
        self.dataunits_ids = [dataunit.id for dataunit in dataunits]

    def update(self, data):
        """Updates dataunit and send to others."""
        if data.get('id') in self.dataunits:
            return 1
        return 0

    def create(self):
        # insert new dataunit depending on the request
        pass

    def get(self):
        # find appropriate dataunit
        pass

    def to_dict(self):
        return dict((field, getattr(self, field)) for field in self.fields)
