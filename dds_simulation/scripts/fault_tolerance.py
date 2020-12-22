from typing import List

import networkx as nx
from networkx.algorithms import minimum_cut

import random

from dds_simulation.scripts.delivery_probabilities import DatalossProbability


def get_star_topology(n):
    return nx.star_graph(n)


class FaultTolerance:
    """
    Find maximum probability of fault to not to lose same delivery
    """

    def __init__(self, graph, cut_nodes: List[tuple]):
        self.graph = graph
        self.cut_nodes = cut_nodes

    def _get_cut_offs(self):
        """Return all minimal cut offs of the network"""
        fault_tolerant_nodes = []
        max = -1
        for x, y in self.cut_nodes:
            cut_value, (reachable, non_reachable) = minimum_cut(self.graph, x, y)
            non_reachable_count = len(non_reachable)
            if max < non_reachable_count:
                max = non_reachable_count
                fault_tolerant_nodes.append((x, y))

            # the more nodes partition has the more fault tolerant should be a node

            import ipdb; ipdb.set_trace()

    def fault_tolerance_graph(self):
        pass


def run():
    graph = get_star_topology(7)

    for node in graph.nodes_iter(data=True):
        graph[node]['p_fault'] = random.random(0, 1)
        # given capacity for each of node (amount data at a time slot in bytes/bits)
        graph[node]['capacity'] = random.random(0, 1)

    # define probabilities for dataloss matrix
    # prob = DatalossProbability(graph)
    # prob.build_adjacency_matrix()
    # prob.build_probabilities_matrix()

    tolerance = FaultTolerance(graph)
open("test.txt")