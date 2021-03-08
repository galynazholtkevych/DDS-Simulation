import argparse
import asyncio
import glob
import json
import random

from networkx import Graph
import sys
import importlib
import os
sys.path.append(os.getcwd())
from dds_simulation.conf import default
from dds_simulation.visualisation import graph_building

from dds_simulation.experiments import simulation
from dds_simulation.experiments import consistent_partitions
from dds_simulation.visualisation.extrapolation import draw_function
from dds_simulation.visualisation.draw_graphics import draw_availability_graph, draw_time_measurements
from dds_simulation.scripts.delivery_probabilities import calculate_delivery_matrix_replication_process


def _initiate_dds(distribution, dataunits_number):
    G = Graph()
    G.add_edge(0, 1)
    G.add_edge(0, 2)
    G.add_edge(2, 1)
    graph = graph_building.form_graph(int(default.parameter('topology', 'nodes')), 5)
    return simulation.DDSMessaging(graph, distribution, dataunits_number)


def experiment_consistency_level(*args, **kwargs):
    nodes = int(default.parameter('topology', 'nodes'))
    degree = int(default.parameter('topology', 'degree'))
    graph = graph_building.form_graph(nodes, degree)
    exp = consistent_partitions.ConsistentExperiment(graph)
    exp.start_experiment()


def experiment_replication(*args, **kwargs):
    x_time_slots_taken_vector = []
    y_graph_diameter_vector = []
    distribution = default.parameter('dataunit', 'distribution')
    dataunits_number = int(default.parameter('dataunit', 'dataunits'))

    for i in range(experiments):
        print("====================")
        print(i)

        dds_service = _initiate_dds(distribution, dataunits_number)
        dds_service.start_experiment()

        x_time_slots_taken_vector.append(dds_service.time_slots_taken)
        y_graph_diameter_vector.append(dds_service.diameter)

    print("X vector is >> ", len(x_time_slots_taken_vector), x_time_slots_taken_vector)
    print("Y>>> ", len(y_graph_diameter_vector), y_graph_diameter_vector)

    draw_function(x_time_slots_taken_vector, y_graph_diameter_vector,
                  'Time taken for consistency to restore', 'Diameter of graph',
                  f'{int(default.parameter("topology", "nodes"))}-weighted-consistency-convergence')


def experiment_replication_delivery_matrix(*args, **kwargs):
    calculate_delivery_matrix_replication_process()


def experiment_controlled_balancing(algorithm_name, **kwargs):
    algorithm_module = importlib.import_module(f'experiments.algorithm_{algorithm_name}.algorithm')

    writes = 400
    nodes = 200
    dataunits = 100
    loop = asyncio.get_event_loop()
    try:
        reads = loop.run_until_complete(algorithm_module.run(nodes_number=nodes,
                                                             dataunits_number=dataunits,
                                                             writes_number=writes))
        path = os.path.join(default.PROJECT_ROOT, 'results', algorithm_name, '*_availability*.json')
        for filename in glob.glob(path):
            with open(filename) as f:
                content = json.loads(f.read())
                draw_availability_graph(writes, reads, content, algorithm_name)
                os.remove(filename)

        path = os.path.join(default.PROJECT_ROOT, 'results', algorithm_name, '*_write_time*.json')
        for filename in glob.glob(path):
            with open(filename) as f:
                content = json.loads(f.read())
                draw_time_measurements(list(content.keys()), list(content.values()),
                                       x_label='Write operation for dataunit', y_label='Time taken',
                                       alg_folder=algorithm_name,
                                       filename=f'write_time_{nodes}_nodes_{len(content.keys())}_operations')
                os.remove(filename)

        path = os.path.join(default.PROJECT_ROOT, 'results', algorithm_name, '*_read_time*.json')
        for filename in glob.glob(path):
            with open(filename) as f:
                content = json.loads(f.read())
                draw_time_measurements(list(content.keys()), list(content.values()),
                                       x_label='Read operation for dataunit', y_label='Time taken',
                                       alg_folder=algorithm_name,
                                       filename=f'read_time_{nodes}_nodes_{len(content.keys())}_operations')
                os.remove(filename)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == '__main__':
    experiments = int(default.parameter('experiment', 'experiments'))
    nodes = int(default.parameter('topology', 'nodes'))

    parser = argparse.ArgumentParser(description='Run DDS experiments.')
    parser.add_argument('experiment', type=str,
                        choices=('consistency_level', 'replication', 'replication_delivery_matrix',
                                 'controlled_balancing'),
                        help='Experiment to run')
    parser.add_argument('-a', '--algorithm', dest='algorithm_name', type=str,
                        choices=('gateway_balancing', 'node_balancing', 'lb_custom'),
                        help='Algorithm implementation to run of controlled balancing method')

    args = parser.parse_args()

    experiment_args = []
    if args.experiment == 'controlled_balancing':
        method_implementation_name = args.algorithm_name
        experiment_args.append(method_implementation_name)

    experiment_name = f'experiment_{args.experiment}'
    globals()[experiment_name](*experiment_args)
