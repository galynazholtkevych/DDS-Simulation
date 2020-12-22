from uuid import uuid1


class Dataunit(object):

    def __init__(self, identity):
        self.fields = {'id', 'uuid', 'data'}
        self.id = identity
        # uuid of the dataunit located on the special node
        self.uuid = uuid1()
        self.data = '{}_some data message'.format(self.id)

    def to_dict(self):
        return dict((k, getattr(k)) for k in self.fields)

    def update(self, new_data):
        for k, v in new_data:
            setattr(k, v, new_data)

