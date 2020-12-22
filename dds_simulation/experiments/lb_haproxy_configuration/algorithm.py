import asyncio
from datetime import datetime
import json
import random
import os
import time

from dds_simulation.conf.default import PROJECT_ROOT
from dds_simulation.experiments.constants import (AVERAGE_TIME_READ, AVERAGE_TIME_WRITE, GRAPH_DEGREE,
                                                  AVAILABILITY_THRESHOLD)


HASH_TABLE_REPLICA = None


async def run(nodes_number, dataunits_number, writes_number):
    """

    :param nodes:
    :return:
    """

    dataunits = [i for i in range(dataunits_number)]
    global HASH_TABLE_REPLICA
    HASH_TABLE_REPLICA = dict.fromkeys(dataunits, 0)
    HaproxySimulation.init_backend(dataunits=dataunits, nodes_number=nodes_number)

    write_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(writes_number)]

    reads_number = int(writes_number * 3 / 2)
    read_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(reads_number)]

    writes = [asyncio.ensure_future(HaproxySimulation.write(request)) for request in write_requests]
    reads = [
        asyncio.ensure_future(HaproxySimulation.read(request)) for request in read_requests]

    spread_tasks_number = (reads_number + writes_number) * 2
    spreads = [asyncio.ensure_future(Node.spread_replica(nodes_number, dataunits_number))
                                for i in range(spread_tasks_number)]

    getting_state = [asyncio.ensure_future(HaproxySimulation.get_state())] * (reads_number // 3)
    coros = writes + reads + spreads + getting_state
    random.shuffle(coros)
    group = asyncio.gather(*coros)

    await group
    return reads_number


class HaproxySimulation:
    AVERAGE_TIME_FIND_NODE = 1  # average time in seconds to find node in usual inherited algorithm
    __instance = None
    backends_mapping = None
    nodes_number = None
    parent = None  # parent lb algorithm (round robin, least connection)

    def __init__(self, *args, **kwargs):
        if HaproxySimulation.__instance is not None:
            raise Exception("This is a singleton")

    @staticmethod
    def getInstance():
        """ Static access method. """
        if HaproxySimulation.__instance is None:
            HaproxySimulation()
        return HaproxySimulation.__instance

    @classmethod
    def init_backend(cls, **kwargs):
        cls.backends_mapping = dict.fromkeys(kwargs['dataunits'], set())
        cls.nodes_number = kwargs['nodes_number']
        for key in cls.backends_mapping:
            cls.backends_mapping[key] = set(random.sample(
                [i for i in range(cls.nodes_number)],
                random.randint(0, cls.nodes_number)
            ))

    @classmethod
    async def get_state(cls):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("GETTING STATE")
        state = {dataunit: len(cls.backends_mapping[dataunit]) for dataunit in cls.backends_mapping}
        print(state.values())

        ts = datetime.fromtimestamp(time.time())
        with open(os.path.join(PROJECT_ROOT, 'results', 'lb_haproxy_configuration',
                               f'alg_{len(cls.backends_mapping.keys())}_dus_{ts}.json'),
                  'w') as f:
            json.dump(state, f)
        return state

    @classmethod
    async def read(cls, request):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("READ")
        du = request.get('dataunit')
        nodes = cls.backends_mapping.get(du)

        if not nodes:
            print("!!!! NOT FOUND")
            return {'status': 404}
        random.choice(list(nodes))  # update with the time that load balance alg parent spends for finding node
        return Node.read(du)

    @classmethod
    async def write(cls, request):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("WRITE")
        du = request.get('dataunit')
        nodes = cls.backends_mapping.get(du)
        if not nodes:
            return {'status': 404}
        random.choice(list(nodes))
        resp = await Node.write(du)
        return resp


class HaproxyRestApplication:

    @classmethod
    async def spread(cls, dataunit, nodes):
        HaproxySimulation.backends_mapping[dataunit] |= set(nodes)

    @classmethod
    async def update_backend_with_new_node_set(cls, dataunit, node):
        # simulate rest api request to haproxy
        HaproxySimulation.backends_mapping[dataunit] = {node}


class Node:
    dataunit = None  # Node knows which dataunit it holds
    neighbors = None  # Node knows what neighbors he has

    @classmethod
    def read(self, dataunit):
        return {'time': AVERAGE_TIME_READ}

    @classmethod
    async def write(cls, du, ts=None):
        node = random.randint(0, HaproxySimulation.nodes_number - 1)
        created_at = ts or time.time()
        if HASH_TABLE_REPLICA.get(du) < created_at:
            HASH_TABLE_REPLICA[du] = created_at
            await HaproxyRestApplication.update_backend_with_new_node_set(du, node)

        return {'status': 201, 'created_at': created_at, 'time': AVERAGE_TIME_WRITE}

    @classmethod
    async def spread_replica(cls, nodes, dataunits):
        # simulate returning neighbors nodes
        await asyncio.sleep(random.uniform(0.5, 10))
        print("SPREAD REPLICA")

        nodes = random.sample([i for i in range(nodes)], GRAPH_DEGREE)
        dataunits = [d for d in HaproxySimulation.backends_mapping if len(HaproxySimulation.backends_mapping[d])
                     < AVAILABILITY_THRESHOLD]
        du = random.choice(dataunits)
        await cls.write(du, ts=0)
        # spend some time to send request
        await HaproxyRestApplication.spread(du, nodes)
