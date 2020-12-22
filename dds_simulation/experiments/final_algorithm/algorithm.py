import random
from random import sample
import time

from dds_simulation.conf import default
UPDATE_TIME = 5  # sec
QUERY_SELECT_EXECUTION_TIME = 3

node_list = [i for i in range(15)]
dataunit_list = [i for i in range(5)]
DATAUNITS_NODES_MAPPING = {dataunit: list(sample(node_list, 5)) for dataunit in dataunit_list}

DATAUNITS_NODES_PARTITION_MAPPING = {dataunit: list(sample(node_list, 8)) for dataunit in dataunit_list}

from dds_simulation.visualisation.draw_graphics import draw_time_measurements


async def run(*args, **kwargs):
    request = {'dataunit': sample(dataunit_list, 1)[0]}
    experiments = list(range(100))
    time_line = []
    # for i in experiments:
    #     print("experiment ", i)
    #     start_time = time.time()
    #     SupposedLoadBalancer().receive(request)
    #     end_time = time.time()
    #     taken_time = round(end_time - start_time, 2)
    #     print("Time taken ", taken_time)
    #     time_line.append(taken_time)
    # print("Time line")
    # print(time_line)
    # draw_time_measurements(experiments, time_line, 'Experiments', 'Time taken', 'final_algorithm', f'read-request-time-{len(experiments)}')

    experiments = list(range(100))
    replication_time_line = []
    from copy import deepcopy
    write_time_line = []
    for i in experiments:
        request = {'dataunit': sample(dataunit_list, 1)[0]}
        req = deepcopy(request)
        replication_start_time = time.time()

        time_line = SupposedLoadBalancer().send(req)
        print("now len of timel ", len(time_line))
        print(len(time_line))
        write_time_line.extend(time_line)
        replication_end_time = time.time()
        print("Experiment ", i)

        print("Now current time line is ::: ", time_line)
        replication_time_line.append(round(replication_end_time - replication_start_time, 10))

    print(write_time_line)
    print("len of timeline ", len(write_time_line))
    # draw_time_measurements([i for i in range(len(write_time_line))], write_time_line, f'Experiments', 'Time taken', 'final_algorithm', f'write-request-time-{len(experiments)*len(node_list)}')
    # draw_time_measurements(experiments, replication_time_line, f'Experiments for {len(node_list)}', 'Time taken', 'final_algorithm', 'full-replication-time')


class SupposedLoadBalancer:
    def send(self, request):
        dataunit = request['dataunit']

        nodes = DATAUNITS_NODES_PARTITION_MAPPING.get(dataunit)
        # sample(nodes, 1)[0]
        request['nodes'] = nodes
        timeline = Node().send(request)
        return timeline

    def receive(self, request):
        print("start receive")
        dataunit = request['dataunit']
        nodes = DATAUNITS_NODES_MAPPING.get(dataunit)
        for node in nodes:
            response = Node().receive()
            if response:
                print("found node")
                sleep_time = random.uniform(QUERY_SELECT_EXECUTION_TIME - 1, QUERY_SELECT_EXECUTION_TIME + 1)
                time.sleep(sleep_time)
                print("end receive")
                return


class Coordinator:
    def __init__(self):
        pass

    def send(self):
        pass

    def receive(self, request_object):
        return random.choice([True, False])


class Node:
    def __init__(self):
        self.write_timeline_replica = []
        self.write_timeline = []
        self.average_write_timeline = []

    def send(self, request):
        degree = 1
        COUNT = 0
        for node in node_list:
            start_time = time.time()
            dataunit = request.get('dataunit')
            nodes = request.get('nodes', [])

            is_replica = request.get('is_replica')
            if is_replica and node in nodes:
                self._replica_write_process(request, dataunit, node)
            elif not is_replica:
                if not nodes:
                    nodes = DATAUNITS_NODES_PARTITION_MAPPING[dataunit]
                if node in nodes:
                    self._write_process(request, nodes, node, dataunit)
            end_time = time.time()
            time_taken = round(end_time - start_time, 2)
            print("Write time taken ", time_taken)
            if time_taken > 0:
                self.average_write_timeline.append(time_taken)
            COUNT += degree
            if COUNT >= len(node_list):
                return self.average_write_timeline

    def _replica_write_process(self, request, dataunit, node):
        start_time = time.time()
        sleep_time = random.uniform(UPDATE_TIME - 1, UPDATE_TIME + 1)
        time.sleep(sleep_time)
        DATAUNITS_NODES_MAPPING[dataunit].append(node)
        nodes = request.get('nodes', [])
        try:
            nodes.remove(node)
        except ValueError:
            pass
        request['nodes'] = nodes

        end_time = time.time()
        self.write_timeline_replica.append(round(end_time - start_time, 2))

    def _write_process(self, request, nodes, node, dataunit):
        start_time = time.time()

        # simulate update
        sleep_time = random.uniform(UPDATE_TIME - 1, UPDATE_TIME + 1)
        time.sleep(sleep_time)

        try:
            nodes.remove(node)
        except ValueError:
            pass
        DATAUNITS_NODES_MAPPING[dataunit] = node
        request['nodes'] = nodes
        end_time = time.time()
        self.write_timeline.append(round(end_time - start_time, 2))

    def receive(self):
        return random.choice([True, False])
