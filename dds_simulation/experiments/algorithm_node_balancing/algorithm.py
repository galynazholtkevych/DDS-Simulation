import asyncio
from datetime import datetime
import json
import os
import random
from random import sample
import time

from dds_simulation.visualisation import graph_building
from dds_simulation.conf import default
from dds_simulation.conf.default import PROJECT_ROOT

UPDATE_TIME = 5  # sec
QUERY_SELECT_EXECUTION_TIME = 3

node_list = [i for i in range(15)]
dataunit_list = [i for i in range(5)]
# DATAUNITS_NODES_MAPPING = {dataunit: list(sample(node_list, 5)) for dataunit in dataunit_list}
# DATAUNITS_NODES_PARTITION_MAPPING = {dataunit: list(sample(node_list, 8)) for dataunit in dataunit_list}

from dds_simulation.visualisation.draw_graphics import draw_time_measurements

CONSTANT_LOCAL_WRITE = 3  # sec
CONSTANT_LOCAL_READ = 1  # sec
AVAILABILITY_MIN_THRESHOLD = 30  # minimum consistent nodes


class Node:
    availability_min_threshold = 30

    def __init__(self, availability_min_threshold, graph, full_replication_evaluation=False):
        self.availability_min_threshold = availability_min_threshold or self.availability_min_threshold
        self.graph = graph
        self.data = dict.fromkeys([i for i in range(50)], 'value')
        self.nodes_processed = []
        self.full_replication_evaluation = full_replication_evaluation

    def _time_measurement(self, dataunit, end_time, start_time, operation_type):
        filename = os.path.join(PROJECT_ROOT, 'results', 'node_balancing',
                               f'alg_node_balancing_{operation_type}_time_{self.graph.number_of_nodes()}_nodes.json')
        try:
            with open(filename) as f:
                content = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            content = {}

        with open(filename, 'w') as f:
            content[dataunit.identifier] = end_time - start_time
            json.dump(content, f)

    async def db_write(self):
        await asyncio.sleep(random.uniform(CONSTANT_LOCAL_WRITE-1, CONSTANT_LOCAL_WRITE+1))

    async def db_read(self):
        await asyncio.sleep(random.uniform(CONSTANT_LOCAL_READ-1, CONSTANT_LOCAL_READ+1))
        return 'value'

    async def read(self, dataunit, node_number=1, request_from_neighbor=False):
        if not self.full_replication_evaluation:
            tasks = asyncio.all_tasks()
            for task in tasks:
                if ('coro=<Node.read() running at' in str(task)
                        or 'coro=<Node.get_state()' in str(task)):
                    break
            else:
                print("Simulation has finished. Cancelling all remaining writes since it does not affect "
                      "algorithm evaluation")
                for task in tasks:
                    task.cancel()

        await asyncio.sleep(random.uniform(0.5, 3))
        start_time = time.time()
        print("Read at node")
        nodes = ConsistentMapping.get_consistent_nodes(dataunit.identifier)
        if node_number in nodes or request_from_neighbor:
            result = await self.db_read()
            end_time = time.time()
            self._time_measurement(dataunit, end_time, start_time, operation_type='read')
            return result

        if nodes and len(nodes) > self.availability_min_threshold:
            # return consistent data
            # choose nodes according the balancing algorithm
            node_identifier_to_send = nodes[random.randint(0, len(nodes)-1)]
            print(f"Sending request to {node_identifier_to_send}")
            return await self.read(dataunit, node_identifier_to_send, request_from_neighbor=True)
        else:
            neighbors = list(self.graph.neighbors(node_number))
            random_node_number = random.randint(0, len(neighbors) - 1)
            node = neighbors[random_node_number]

            # await asyncio.sleep(random.uniform(1, 2))
            print(f"Sending request to {node}")
            return await self.read(dataunit, node, request_from_neighbor=True)

    async def write(self, dataunit, value, node_number=1, start_time=None):
        if not self.full_replication_evaluation:
            tasks = asyncio.all_tasks()
            for task in tasks:
                if ('read() running at' in str(task)
                        or 'get_state()' in str(task)):
                    break
            else:
                print("Simulation has finished. Cancelling all remaining writes since it does not affect "
                      "algorithm evaluation")
                for task in tasks:
                    task.cancel()

        await asyncio.sleep(random.uniform(0.5, 3))
        print("Write at node")
        await self.db_write()

        written_dataunit_result = ConsistentMapping.add_consistent_node(dataunit, node_number)

        if written_dataunit_result == 409:
            print(
                f"HTTP 409: The data for this dataunit {dataunit.identifier} are already written to database after your request.")
            return

        self.data[dataunit] = value
        self.nodes_processed.append(node_number)
        # await asyncio.sleep(0)
        if len(self.nodes_processed) > self.graph.number_of_nodes():
            end_time = time.time()
            self._time_measurement(dataunit, end_time, start_time, operation_type='write')
            return

        neighbors = self.graph.neighbors(node_number)
        neighbors = list(set(neighbors) - set(self.nodes_processed))
        if not neighbors:
            # end_time = time.time()
            # self._time_measurement(dataunit, end_time, start_time, operation_type='write')
            return
        self.nodes_processed.extend(neighbors)
        node = neighbors[random.randint(0, len(neighbors) - 1)]
        await self.write(dataunit, value, node, start_time)

    async def get_state(self, reads_number, writes_number):
        await asyncio.sleep(random.uniform(0.5, 3))
        print("GETTING STATE")

        ts = datetime.fromtimestamp(time.time())
        with open(os.path.join(PROJECT_ROOT, 'results', 'node_balancing',
                               f'alg_node_balancing_availability_{writes_number}_w_{reads_number}_r_{ts}.json'),
                  'w') as f:
            state = await ConsistentMapping.get_state()
            print(state)
            json.dump(state, f)


class Dataunit:
    def __init__(self, ts, dataunit_id):
        self.ts = ts
        self.identifier = dataunit_id

    def __hash__(self):
        return hash(self.identifier)

    def __eq__(self, other):
        return self.identifier == other


class ConsistentMapping:
    mapping = None

    @classmethod
    def get_dataunit(cls, dataunit):
        for item in cls.mapping.keys():
            if item == dataunit.identifier:
                return item

    @classmethod
    def get_consistent_nodes(cls, dataunit):
        nodes = cls.mapping.get(dataunit)
        return nodes

    @classmethod
    def add_consistent_node(cls, dataunit, node):
        established_dataunit = cls.get_dataunit(dataunit)
        if established_dataunit is None or established_dataunit.ts < dataunit.ts:
            cls.mapping.pop(established_dataunit, None)
            cls.mapping[dataunit] = [node, ]
            return "new"
        elif established_dataunit.ts == dataunit.ts:
            if node not in cls.mapping[established_dataunit]:
                cls.mapping[established_dataunit].append(node)
            return "added"
        elif established_dataunit.ts > dataunit.ts:
            print("established dataunit when 409 : ", established_dataunit.ts, dataunit.ts)
            return 409

    @classmethod
    async def get_state(cls):
        state = {dataunit.identifier: len(cls.get_consistent_nodes(dataunit)) for dataunit in cls.mapping}
        return state


async def run(nodes_number, dataunits_number, writes_number):
    network_graph = graph_building.form_graph(nodes_number, degree=7)
    dataunits = [Dataunit(dataunit_id=i, ts=1) for i in range(dataunits_number)]
    ConsistentMapping.mapping = {dataunit: list(sample(list(network_graph.nodes()),
                                                       random.randint(0, network_graph.number_of_nodes())))
                                 for dataunit in dataunits}

    write_requests = [Dataunit(dataunit_id=random.randint(1, dataunits_number - 1), ts=None)
                      for i in range(writes_number)]
    for dataunit in write_requests:
        dataunit.ts = 2

    reads_number = int(writes_number // 2)
    read_requests = [Dataunit(dataunit_id=random.randint(1, dataunits_number - 1), ts=None)
                     for i in range(reads_number)]

    node = Node(availability_min_threshold=AVAILABILITY_MIN_THRESHOLD, graph=network_graph)
    value = 'some_value'
    start_time = time.time()
    writes = [node.write(dataunit, value, start_time=start_time) for dataunit in write_requests]
    reads = [node.read(dataunit) for dataunit in read_requests]

    getting_state = [node.get_state(reads_number, writes_number)] * int(writes_number // 2)

    coros = writes + reads + getting_state
    random.shuffle(coros)
    try:
        group = asyncio.gather(*coros)
        await group
    except asyncio.exceptions.CancelledError:
        pass

    return reads_number
