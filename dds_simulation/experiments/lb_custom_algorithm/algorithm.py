import asyncio
from datetime import datetime
import json
import random
import os
import time

from dds_simulation.experiments.constants import (AVERAGE_TIME_READ, AVERAGE_TIME_WRITE,
                                                  GRAPH_DEGREE, AVAILABILITY_THRESHOLD)
from dds_simulation.conf.default import PROJECT_ROOT


async def run(nodes_number, dataunits_number, writes_number):
    """

    :param nodes:
    :return:
    """
    # how many request
    # add many dataunits (1000)
    dataunits = [i for i in range(dataunits_number)]
    load_balancer = LBCustomBalanceSimulation(
        storage=HAshTableSimulation(dataunits, nodes_number))

    write_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(writes_number)]

    reads_number = int(writes_number * 2 / 3)
    read_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(reads_number)]
    writes = asyncio.gather(*([asyncio.ensure_future(load_balancer.write(request)) for request in write_requests]))
    reads = asyncio.gather(*([
        asyncio.ensure_future(load_balancer.read(request)) for request in read_requests]))

    spread_tasks_number = (reads_number + writes_number) * 2
    spreads = asyncio.gather(*([asyncio.ensure_future(load_balancer.spread()) for i in range(spread_tasks_number)]))

    getting_state = asyncio.gather(*([asyncio.ensure_future(load_balancer.get_state())] * 10))

    group = asyncio.gather(writes, reads, spreads, getting_state)

    await group
    return reads_number


class HAshTableSimulation:
    keys = []
    storage = {}
    replicas = {}

    def __init__(self, dataunits, nodes):
        self.nodes_number = nodes
        self.dataunits_number = len(dataunits)
        self.keys = dataunits
        self.storage = dict.fromkeys(dataunits, set())
        self.replicas = dict.fromkeys(dataunits, 0)

        for key in self.storage:
            self.storage[key] = set(random.sample(
                [i for i in range(self.nodes_number)],
                random.randint(0, self.nodes_number)
            ))

    def add(self, dataunit, node, ts=0):
        """Updates storage if given replica is newest"""
        if self.replicas.get(dataunit, 0) < ts:
            self.replicas[dataunit] = ts
            self.delete(dataunit)

        if isinstance(node, list):
            self.storage[dataunit] |= set(node)
        else:
            self.storage[dataunit].add(node)

    def delete(self, dataunit):
        self.storage[dataunit] = set()

    def get_state(self):
        state = {dataunit: len(self.storage[dataunit]) for dataunit in self.storage}
        print(state.values())
        return state


class LBCustomBalanceSimulation:
    AVERAGE_TIME_FIND_NODE = 1  # average time in seconds to find node in usual inherited algorithm
    __instance = None
    storage = None
    parent = None  # parent lb algorithm (round robin, least connection)

    def __init__(self, *args, **kwargs):
        if LBCustomBalanceSimulation.__instance is not None:
            raise Exception("This is a singleton")

        self.storage = kwargs.get('storage')

    @staticmethod
    def getInstance():
        """ Static access method. """
        if LBCustomBalanceSimulation.__instance is None:
            LBCustomBalanceSimulation()
        return LBCustomBalanceSimulation.__instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    async def get_state(self):
        await asyncio.sleep(random.uniform(0.5, 10))
        print("GETTING STATE")
        ts = datetime.fromtimestamp(time.time())
        with open(os.path.join(PROJECT_ROOT, 'results', 'lb_custom_algorithm',
                               f'alg_{len(self.storage.keys)}_dus_{2000}_w_{3000}_r_{ts}.json'),
                  'w') as f:
            json.dump(self.storage.get_state(), f)

        return

    async def read(self, request):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("READ")
        du = request.get('dataunit')
        nodes = self.storage.storage.get(du)

        if not nodes:
            print("!!!! NOT FOUND")
            return {'status': 404}
        random.choice(list(nodes))  # update with the time that load balance alg parent spends for finding node
        await asyncio.sleep(self.AVERAGE_TIME_FIND_NODE)
        return Node.read(du)

    async def write(self, request):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("WRITE")
        du = request.get('dataunit')
        node = random.randint(0, self.storage.nodes_number - 1)
        resp = Node.write(du)
        if resp.get('status') == 201:
            self.storage.add(du, node, resp.get('created_at'))
        return resp

    async def spread(self):
        """update table with current node"""
        await asyncio.sleep(random.uniform(0.5, 5))
        print("SPREAD")
        dataunits = [d for d in self.storage.replicas if len(self.storage.storage[d]) < AVAILABILITY_THRESHOLD]
        dataunit = random.sample(dataunits, 1)[0] if dataunits else random.randint(0, self.storage.dataunits_number - 1)

        node_ids = Node.spread_replica(self.storage.nodes_number)
        # ts = 0 just to not update with the newest replica, but simulate spreading , accumulating set of nodes by du
        self.storage.add(dataunit, node_ids, ts=0)
        return


class Node:
    """Simulation of database instance"""

    @classmethod
    def read(cls, dataunit):
        return {'time': AVERAGE_TIME_READ}

    @classmethod
    def write(cls, dataunit):
        return {'status': 201, 'created_at': time.time(), 'time': AVERAGE_TIME_WRITE}

    @classmethod
    def spread_replica(cls, nodes_number):

        # simulate returning neighbors nodes
        return random.sample([i for i in range(nodes_number)],
                             GRAPH_DEGREE)
