import asyncio
from datetime import datetime
import json
import os
import random
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
    DBMiddleware.init_app(nodes_number=nodes_number, dataunits=dataunits)

    write_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(writes_number)]

    reads_number = int(writes_number * 2 / 3)
    read_requests = [{'dataunit': random.randint(0, dataunits_number - 1)} for i in range(reads_number)]

    writes = [LBSimulation.write(request) for request in write_requests]
    reads = [LBSimulation.read(request) for request in read_requests]

    spread_tasks_number = (reads_number + writes_number) // 10
    spreads = [asyncio.ensure_future(DBMiddleware._spread()) for i in range(spread_tasks_number)]
    getting_state = [(DBMiddleware.get_state())] * spread_tasks_number
    coros = reads + getting_state + writes + spreads
    random.shuffle(coros)
    results = await asyncio.gather(*coros)
    print(results)
    return reads_number


class LBSimulation:
    @classmethod
    async def read(cls, request):
        # choose node to forward on lb level
        await asyncio.sleep(random.uniform(0.1, 0.2))
        print("READ")
        return await Node.read(request)

    @classmethod
    async def write(cls, request, ts=None):
        # choose node to forward on lb level
        await asyncio.sleep(random.uniform(0.1, 0.4))
        print("WRITE")
        return await Node.write(request)


class Node:
    """Simulation of database instance"""

    @classmethod
    async def read(cls, request):
        return await DBMiddleware.read(request)

    @classmethod
    async def write(cls, request, ts=None):
        await DBMiddleware.write(request)


class DBMiddleware:
    replicas = None
    nodes_number = None
    dataunits = None
    cnode_map = None
    dnode_map = None

    @classmethod
    def init_app(cls, **kwargs):
        cls.nodes_number = kwargs.get('nodes_number')
        cls.dataunits = kwargs.get('dataunits')

        cls.dnode_map = dict.fromkeys(cls.dataunits, set())
        cls.cnode_map = dict.fromkeys(cls.dataunits, set())

        for key in cls.dnode_map:
            cls.dnode_map[key] = set(random.sample(
                [i for i in range(cls.nodes_number)],
                random.randint(0, cls.nodes_number)
            ))

        for key in cls.cnode_map:
            cls.cnode_map[key] = set(random.sample(
                [i for i in range(cls.nodes_number)],
                random.randint(0, cls.nodes_number)
            ))

        cls.replicas = dict.fromkeys(cls.dataunits, 0)

    @classmethod
    async def get_state(cls):
        await asyncio.sleep(random.uniform(0.5, 5))
        print("GETTING STATE")
        state = {dataunit: len(cls.cnode_map[dataunit]) for dataunit in cls.cnode_map}
        print(state.values())

        ts = datetime.fromtimestamp(time.time())
        with open(os.path.join(PROJECT_ROOT, 'results', 'own_balancing',
                               f'alg_{len(cls.cnode_map.keys())}_dus_{ts}.json'),
                  'w') as f:
            json.dump(state, f)
        return state

    @classmethod
    def update(cls, dataunit, node, ts=0.):
        """Updates storage if given replica is newest"""

        if ts > 0 and cls.replicas.get(dataunit, 0) < ts:
            cls.replicas[dataunit] = ts
            cls.cnode_map[dataunit] = set()

        if isinstance(node, list):
            cls.cnode_map[dataunit] |= set(node)
        elif isinstance(node, set):
            cls.cnode_map[dataunit] |= node
        else:
            cls.cnode_map[dataunit].add(node)

    @classmethod
    async def read(cls, request):

        du = request.get('dataunit')
        if request.get('internal'):
            await asyncio.sleep(random.uniform(0.5, 1))  # execute query
            return {'time': AVERAGE_TIME_READ}

        nodes = DBMiddleware.cnode_map.get(du)
        current_node = random.randint(0, DBMiddleware.nodes_number)
        if current_node in nodes:
            await asyncio.sleep(random.uniform(0.5, 1))  # execute query
            return {'time': AVERAGE_TIME_READ}

        # get fault tolerant list
        fault_tolerant_nodes = set(random.sample(
            [i for i in range(cls.nodes_number)],
            random.randint(0, cls.nodes_number)
        ))
        broadcast_list = set(nodes) - fault_tolerant_nodes
        request['internal'] = True
        request['broadcast_list'] = broadcast_list
        if broadcast_list:
            return await cls.read(request)
        # here comes not strict consistency
        broadcast_list = set(cls.dnode_map.get(du)) - fault_tolerant_nodes
        await asyncio.sleep(random.uniform(0.5, 1))  # execute query
        return {"time": AVERAGE_TIME_READ}

    @classmethod
    async def write(cls, request, ts=None):
        created_at = ts or time.time()
        du = request.get('dataunit')
        nodes = request.get('nodes', [])# or random.randint(0, Application.nodes_number - 1)

        if request.get('internal'):
            # if re quest is internal this node certainly has the dataunit coming
            await asyncio.sleep(random.uniform(0.5, 1))  # execute query
            if nodes:
                cls.update(du, nodes, ts=created_at)
                nodes.pop()
                return {'status': 201, 'created_at': created_at, 'time': AVERAGE_TIME_WRITE}

            await cls._spread()
            # do not take here replication
            # if nodes:
            #     request['broadcast_list'] = nodes
            #     # return await cls.write(request)
            return {'status': 201, 'created_at': created_at, 'time': AVERAGE_TIME_WRITE}

        nodes = cls.dnode_map.get(du)
        if not nodes:
            return {'status': 404}
        # update itself
        node = random.randint(0, cls.nodes_number)

        if nodes and node in nodes:
            await asyncio.sleep(random.uniform(0.5, 1))  # execute query
            cls.update(du, node, ts=created_at)

        # await cls._spread()
        # if nodes:
        #     request['internal'] = True
        #     request['broadcast_list'] = nodes
        #     await cls.write(request)

        return {'status': 201, 'created_at': created_at, 'time': AVERAGE_TIME_WRITE}

    @classmethod
    async def _spread(cls):
        canidates_to_update = [d for d in cls.dataunits if len(cls.dnode_map[d]) < AVAILABILITY_THRESHOLD]
        await cls.write({'dataunit': random.choice(canidates_to_update),
                         'nodes': random.sample([i for i in range(cls.nodes_number)],
                                                GRAPH_DEGREE), 'internal': True},
                        ts=0)

