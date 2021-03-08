from decimal import Decimal
from functools import reduce
from operator import mul, itemgetter
import numpy as np
import os
import pprint
import random
from uuid import uuid4

import networkx as nx

PROBABILITY_METRIC_NAME = 'probability'
DELIVERY_METRIC_NAME = 'delivery'
PARTITION_TOLERANCE_NAME = 'partition_tolerance'


class DatalossProbability:
    """
    Class that calculates mean average for message delivery time and probability of delivery

    Formula used:
    1.
    2.
    3.
    TODO: write formula that are used to calculate metrics
    """

    def __init__(self, initial_graph, additional_metrics=(DELIVERY_METRIC_NAME,)):

        # the first metric is required since it is used for calculating other metrics
        self.calculate_delivery = DELIVERY_METRIC_NAME in additional_metrics
        self.calculate_partition_tolerance = PARTITION_TOLERANCE_NAME in additional_metrics

        self.graph = initial_graph

        # we will use graph as the matrix to store its values cause
        # it has iterable implementation to save performance
        self.final_graph = nx.Graph()

        self.threshold_graph = nx.Graph()

    def initialize(self):
        """Build matrix with direct links probabilities"""

        graph_len = self.graph.number_of_nodes()
        for i in self.graph.nodes_iter():
            neighbors = self.graph.neighbors(i)

            # Matrix is symmetric
            for j in range(i, graph_len):
                # those links that are not adjacent will be calculated later when paths are calculated
                adjacency_value, delivery_value = -1, -1

                if j == i:
                    adjacency_value = 1
                    delivery_value = 0

                elif j in neighbors:
                    edge_data = self.graph.get_edge_data(i, j)
                    adjacency_value = edge_data['probability']
                    delivery_value = edge_data['delivery']

                self.final_graph.add_edge(i, j,
                                          probability=adjacency_value,
                                          delivery=delivery_value,
                                          real_delivery=delivery_value)

    def _edges(self, path):
        for index, node in enumerate(path[:-1]):
            yield path[index], path[index + 1]

    def _path_delivery_probability(self, paths):
        """
        Iterable for probabilityies of dataloss for all paths from i to j

        :param paths: paths from i to j
        :return:
        """
        # p data loss is probability of not delivery for all simple paths from i to j
        for path in paths:
            j_probability = 1
            p_node_delivery = 1
            for edge in self._edges(path):
                j_probability *= self.graph.get_edge_data(*edge)['probability']

            if self.calculate_partition_tolerance:
                for node in path:
                    p_node_delivery *= self.graph.node[node]['alive_probability']

            # include to dataloss probability the probability of node failure
            p_delivery = j_probability * p_node_delivery

            yield p_delivery

    def _calculate_mean_delivery(self, paths, p_deliveries, p_delivery_at_least_one):
        """
        Calculates mean delivery time for all paths from i to j
        :param i: node i
        :param j:  node j
        :param paths: all paths from i to j
        :param p_deliveries:
        :return:
        """
        def _path_delivery(paths):
            for path in paths:
                j_delivery = 0
                for edge in self._edges(path):
                    j_delivery += self.graph.get_edge_data(*edge)['delivery']
                yield j_delivery

        t_deliveries = list(_path_delivery(paths))
        p_deliveries = list(map(lambda x: x / p_delivery_at_least_one, p_deliveries))

        mean_delivery = sum(x * y for x, y in zip(p_deliveries, t_deliveries))

        max_probability = p_deliveries.index(max(p_deliveries))
        preferable_delivery = t_deliveries[max_probability]
        return mean_delivery, preferable_delivery

    def __call__(self):
        """Build the matrix with probabilities of delivery from i to j through at least one path"""
        for i in self.graph.nodes_iter():
            nodes = (j for j in self.graph.nodes_iter()
                     if self.final_graph[i][j]['probability'] == -1 and self.final_graph[i][j]['delivery'] == -1)
            nodes = list(nodes)
            for j in nodes:
                paths = nx.all_simple_paths(self.graph, i, j)
                paths = list(paths)
                # calculate the delivery probability for every path from i to j
                p_deliveries_one_of = list(self._path_delivery_probability(paths))
                # p_dataloss for every path from i to j
                p_dataloss = list(map(lambda x: 1 - x, p_deliveries_one_of))
                # p of delivery at least one
                # it is (1 - sum of dataloss probability of ALL of simple paths from i to j)
                dataloss_probability_for_all_paths = reduce(mul, p_dataloss)
                p_delivery_at_least_one = 1 - dataloss_probability_for_all_paths
                if p_delivery_at_least_one == 0:
                    print("!!!!!!!!!At least one probability delivery: ", p_delivery_at_least_one)
                    self.final_graph[i][j]['probability'] = self.final_graph[i][j]['probability'] = 0
                    continue

                # this does not take part in further formula
                # final_probability = sum(p_deliveries_one_of) / p_delivery_at_least_one
                # self.final_graph[i][j]['probability'] = self.final_graph[i][j]['probability'] = (
                #         final_probability
                # )
                if self.calculate_delivery:
                    mean_delivery, preferrable_delivery = self._calculate_mean_delivery(
                        paths, p_deliveries_one_of, p_delivery_at_least_one)
                    self.final_graph[i][j]['delivery'] = self.final_graph[i][j]['delivery'] = mean_delivery
                    self.final_graph[i][j]['real_delivery'] = self.final_graph[i][j]['real_delivery'] = preferrable_delivery

    def represent_delivery(self):
        print("MEAN DELIVERY")
        print_matrix(nx.to_numpy_matrix(self.final_graph, weight='delivery'))
        print("PREFERRABLE DELIVERY")
        print_matrix(nx.to_numpy_matrix(self.final_graph, weight='real_delivery'))

    def calculate_mean_delivery(self):
        delivery = 0
        real_delivery = 0
        for _, _, edge in self.final_graph.edges_iter(data=True):
            delivery += edge['delivery']
            real_delivery += edge['real_delivery']
        mean_delivery = delivery / self.final_graph.number_of_edges()
        mean_real_delivery = real_delivery / self.final_graph.number_of_edges()
        print("AVERAGE--> ", mean_delivery)
        return mean_delivery

    @property
    def result(self):
        return self.final_graph


class MetricsMeasurer:
    """
    Calculates metrics for delivery time with network partitions for given network topology

    """

    def __init__(self, graph, mean_delivery_threshold=5):
        """

        :param graph: graph with edge data of probability of delivery via every edge
        :param mean_delivery_threshold: threshold of mean delivery through network set by administrator
        """
        self.graph = graph
        self.mean_delivery_threshold = mean_delivery_threshold
        self.metrics = {}

    def find_initial_thresholds(self):
        """
        Find thresholds of mean delivery
        :return:
        """
        mean_delivery = self._find_delivery()
        if mean_delivery > self.mean_delivery_threshold:
            print("Warning: Your initial delivery probabilities for edges do not satisfy your set threshold")

    def _find_delivery(self):
        delivery_metric = DatalossProbability(self.graph,
                                              additional_metrics=(DELIVERY_METRIC_NAME, PARTITION_TOLERANCE_NAME))
        delivery_metric.initialize()
        delivery_metric()
        delivery_metric.represent_delivery()
        self.metrics['delivery'] = delivery_metric.result
        return delivery_metric.calculate_mean_delivery()

    def _find_mean_fault_tolerance(self, alive_probability, decrease_step, mean_delivery, node):
        alive_probability -= decrease_step
        wanted_alive_probability = 1
        while mean_delivery <= self.mean_delivery_threshold and alive_probability >= 0.5:
            _add_probability_metric_to_graph_node(self.graph, node, 'alive_probability', alive_probability)
            mean_delivery = self._find_delivery()
            # why mean delivery is still decreasing
            wanted_alive_probability = round(alive_probability, 2)
            alive_probability -= decrease_step

        return wanted_alive_probability, mean_delivery

    def find_fault_tolerance_threshold(self):
        nodes_degrees = sorted(self.graph.degree_iter(), key=itemgetter(1))
        mean_delivery = self.mean_delivery_threshold - 0.1
        alive_probability = 1.0
        decrease_alive_probability_step = 0.1

        alive_probabilities = {}
        # we sort nodes by importance. the first one has maximum degree
        for node, degree in nodes_degrees:
            threshold_fault_tolerance, mean_delivery = self._find_mean_fault_tolerance(
                alive_probability, decrease_alive_probability_step, mean_delivery, node)
            alive_probabilities[node] = (threshold_fault_tolerance, degree)
        threshold_fault_tolerance, mean_delivery = self._find_mean_fault_tolerance()

        return alive_probabilities


# Functions to form necessary metrics that are given as input. for real network another way to form data should be used
def _add_natural_metric_to_graph_edges(graph, metric_name, absolute=None):
    for _, _, edge in graph.edges_iter(data=True):
        # Is taken care of network administrator, calculated by QoS
        edge[metric_name] = absolute or round(random.uniform(1, 3), 1)


def _add_probability_metric_to_graph_edges(graph, metric_name, absolute=False):
    for _, _, edge in graph.edges_iter(data=True):
        # Is taken care of network administrator, calculated by QoS
        edge[metric_name] = int(absolute) or round(random.uniform(0.3, 1), 2)


def _add_probability_metric_to_graph_nodes(graph, metric_name, absolute=False):
    for _, node in graph.nodes_iter(data=True):
        # Is taken care of network administrator, calculated by QoS
        node[metric_name] = int(absolute) or round(random.uniform(0.7, 1), 2)


def _add_probability_metric_to_graph_node(graph, node, metric_name, value):
    # nx.set_node_attributes(graph, {node: })
    graph.node[node][metric_name] = value


def print_matrix(matrix):
    for row in matrix:
        print(f'| {row}\t | ')


def calculate_delivery_matrix_replication_process():

    G = nx.Graph()
    G.add_edge(0, 1)
    G.add_edge(0, 2)
    G.add_edge(2, 1)
    G.add_edge(1, 3)
    G.add_edge(2, 4)
    G.add_edge(0, 5)
    G.add_edge(2, 5)
    #
    d = G.degree()
    MEAN_DELIVERY_THRESHOLD = 7  # chosen by network administrator. calculated using QoS
    # G = nx.random_regular_graph(3, 10)

    _add_probability_metric_to_graph_edges(G, 'probability')
    _add_natural_metric_to_graph_edges(G, 'delivery')
    _add_probability_metric_to_graph_nodes(G, 'alive_probability', absolute=True)

    probability_adj_matrix = nx.to_numpy_matrix(G, weight='probability')
    delivery_adj_matrix = nx.to_numpy_matrix(G, weight='delivery')
    # alive_probability_node_matrix = nx.to_numpy_matrix(G, weight='alive_probability')
    print("INITIAL probability of delivery between neighbors")
    print_matrix(probability_adj_matrix)
    print("INITIAL delivery between neighbors")
    print_matrix(delivery_adj_matrix)

    metrics = MetricsMeasurer(G, MEAN_DELIVERY_THRESHOLD)
    metrics.find_initial_thresholds()
    metrics.graph = G

    # _add_probability_metric_to_graph_nodes(G, 'alive_probability')
    # metrics = MetricsMeasurer(G, MEAN_DELIVERY_THRESHOLD)
    # metrics.find_initial_thresholds()
    # recommended_alive_probabilities = metrics.find_fault_tolerance_threshold()
    # print("Minimum alive probabilities for each of node:")

    # pprint.pprint(recommended_alive_probabilities)
