import asyncio
import glob
import json
import random

import networkx
import sys
import importlib
import os
sys.path.append(os.getcwd())
from dds_simulation.conf import default
from dds_simulation.visualisation import graph_building

from dds_simulation.experiments import simulation
from dds_simulation.experiments import consistent_partitions
from dds_simulation.visualisation.extrapolation import draw_function
from dds_simulation.visualisation.draw_graphics import draw_availability_graph


def _initiate_dds(distribution, dataunits_number):
    G = networkx.Graph()
    G.add_edge(0, 1)
    G.add_edge(0, 2)
    G.add_edge(2, 1)
    degree = random.randint(5, 8) // 2 * 2
    nodes = degree * 7
    graph = graph_building.form_graph(100, 5)
    return simulation.DDSMessaging(graph, distribution, dataunits_number)
    # degree = random.randint(2, 5) // 2 * 2
    # nodes = degree * 7
    # graph = graph_building.form_graph(6, 3)
    #
    # try:
    #     return simulation.DDSMessaging(graph, distribution,
    #                                           dataunits_number)
    # except networkx.exception.NetworkXError:
    #     return _initiate_dds(distribution, dataunits_number)


if __name__ == '__main__':

    if len(sys.argv) <= 1:
        nodes = int(default.parameter('topology', 'nodes'))
        edges = int(default.parameter('topology', 'links'))
        degree = int(default.parameter('topology', 'degree'))
        experiments = int(default.parameter('experiment', 'experiments'))

        dataunits_number = int(default.parameter('dataunit', 'dataunits'))
        distribution = default.parameter('dataunit', 'distribution')
        x_vector = []
        y_vector = []

        for i in range(experiments):
            print("====================")
            print(i)

            dds_service = _initiate_dds(distribution, dataunits_number)
            dds_service.start_experiment()

            x_vector.append(dds_service.time_slots_taken)
            y_vector.append(dds_service.diameter)
        print("X>> ", len(x_vector),x_vector)
        print("Y>>> ", len(y_vector), y_vector)

        draw_function(x_vector, y_vector, 'Time taken for consistency to restore', 'Diameter of graph',
                      f'{experiments}-weighted-consistency-convergence')
        # graph = graph_building.form_graph(nodes, degree)
        # exp = consistent_partitions.ConsistentExperiment(graph)
        # exp.start_experiment()

    # module = 'final_algorithm'#sys.argv[1]
    # algorithm_module = importlib.import_module(f'experiments.{module}.algorithm')
    #
    # writes = 10
    # nodes = 500
    # dataunits = 100
    # loop = asyncio.get_event_loop()
    # try:
    #     reads = loop.run_until_complete(algorithm_module.run(nodes_number=nodes, dataunits_number=dataunits,
    #                                                          writes_number=writes))
    #     path = os.path.join(default.PROJECT_ROOT, 'results', module, '*.json')
    #     for filename in glob.glob(path):
    #         with open(filename) as f:
    #             content = json.load(f)
    #
    #             draw_availability_graph(writes, reads, content, module)
    #             os.remove(filename)
    # finally:
    #     # loop.run_until_complete(loop.shutdown_asyncgens())
    #     loop.close()


