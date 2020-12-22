import abc
import random

import networkx

from dds_simulation.experiments.node import Node
from dds_simulation.experiments.link import Link
from dds_simulation.experiments.dataunit import Dataunit

from dds_simulation.visualisation import graph_control, extrapolation
from dds_simulation.conf import default


class DDS(object):
    """
    Base class for all experiments
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, graph):
        self.nodes = list()
        self.links = set()
        self.graph = graph
        self.nodes = []
        for node_ident in graph.nodes():
            neighbors = graph.neighbors(node_ident)
            self.nodes.append(Node(node_ident, neighbors))

        for node1, node2 in graph.edges():
            self.links.add(Link(node1, node2))
            int(default.parameter('topology', 'nodes'))

    @abc.abstractmethod
    def start_experiment(self):
        """Starting one of experiments"""


class DDSMessaging(DDS):
    """
    Class simulating messaging broadcasting across DDS
    """

    def __init__(self, graph, distribution, dataunits_number):
        super(DDSMessaging, self).__init__(graph)
        self.controller = graph_control.GraphController(graph)
        self.graph = graph
        self.time_slots_taken = 0

        self.nodes = []

        weighted = bool(default.parameter('topology', 'weighted'))
        eccentricity = {}
        for node1, node2 in graph.edges():
            self.links.add(Link(node1, node2))
            if weighted:
                graph[node1][node2]['weight'] = random.randint(1, 9)
            else:
                graph[node1][node2]['weight'] = 1

        for node_ident in graph.nodes():
            neighbors = graph.neighbors(node_ident)
            self.nodes.append(Node(node_ident, neighbors))
            self._eccentricity(node_ident, eccentricity)

        self.diameter = networkx.diameter(graph, e=eccentricity)
        self.dataunits = DDSMessaging.generate_data(dataunits_number)
        self.distribute_data(distribution)

    def _eccentricity(self, node, eccentricity):
        path = networkx.single_source_dijkstra_path_length(
            self.graph, node, weight='weight')
        path = {k: v for k, v in path.items()
                if not eccentricity.get(k) or v > eccentricity[k]}
        eccentricity.update(path)

    @staticmethod
    def generate_data(dataunits):
        return [Dataunit(i) for i in range(dataunits)]

    def distribute_data(self, distribution):
        # NOTE(galyna): later on make separate module distribution
        if distribution == 'random':
            dataunits_per_node = random.sample(
                self.dataunits,
                random.randint(1, len(self.dataunits))
            )
            for node in self.nodes:
                node.add_dataunits(dataunits_per_node)

    def start_experiment(self):
        self.simulate_message()

    def _simulate_message_single_iteration(self, node_identity,
                                           nodes_processed):
        neighbors = self.graph.neighbors(node_identity)
        print("Node: ", node_identity)
        print("NEIGHBORS: ", neighbors)

        # assume now that go to each node takes 1 ts, broadcasting parallel
        #print("ts taken>>> ", self.time_slots_taken)

        nodes_processed.update(set(neighbors))
        nodes_processed.update(set([node_identity, ]))
        return neighbors

    def _simulate_parallel_iterations(self):
        pass

    def _shortest_path_for_neighbors(self, i, neighbors):
        if i in neighbors:
            neighbors.remove(i)
        path = networkx.single_source_dijkstra_path(
            self.graph, i, weight='weight')

        path.pop(i)
        return [path[n][1] for n in neighbors if len(path[n]) > 1]

    def simulate_message(self):
        """Simulate communication process.

        Simulate communication process starting from one of
        nodes.
        """
        # only first node is taken at random
        node_identity = random.randint(0, len(self.nodes) - 1)
        self.time_slots_taken = 0

        new_neighbors = set()
        nodes_processed = set([node_identity])
        print("node ident>>> ", node_identity)
        neighbors = self.graph.neighbors(node_identity)
        path_ahead = self._shortest_path_for_neighbors(node_identity, neighbors)
        print("neighbors>>> ", neighbors)
        nodes_processed.update(set(neighbors))
        link_cost = min([self.graph[node_identity][n]['weight'] for n in path_ahead])
        print("time slots taken first time>>> ", link_cost)
        self.time_slots_taken += link_cost

        while len(nodes_processed) < len(self.nodes):

            # ноды должны тоже выбираться параллельно,
            # а не только одна, должны передавать сообщение
            # ВСЕ соседи ПАРАЛЛЕЛЬНО
            link_costs = []
            print("====================================")

            for i in neighbors:
                print("identity now>>>> ", i)
                neighbors_candidates = set(self.graph.neighbors(i)) - nodes_processed
                new_neighbors.update(neighbors_candidates)
                print("neighbors now>>>> ", new_neighbors)
                path_ahead = self._shortest_path_for_neighbors(
                    i, new_neighbors)
                print("path ahed>>> ", path_ahead)
                if not path_ahead:
                    continue
                max_weight = min([self.graph[i][n]['weight'] for n in path_ahead])
                print("max weight now>>> ", max_weight)
                link_costs.append(
                    max_weight)
            print("=============================")
            print("link costs>> ", link_costs)

            link_cost = min(link_costs)
            print("link costs before addig >> ", link_cost)
            nodes_processed.update(set(new_neighbors))
            self.time_slots_taken += link_cost

            print("current time slots taken>>> ", self.time_slots_taken)

            neighbors.clear()
            link_costs.clear()

            neighbors = list(new_neighbors)[:]
            print("neighbors in the end>>>> ", neighbors)
            new_neighbors.clear()

        if self.time_slots_taken > self.diameter:
            labels = {k.identity: k.identity for k in self.nodes}
            self.controller.draw_graph(
                self.graph, labels, f'{self.time_slots_taken}-{self.diameter}')

    def get_delivery_time(self):
        """Useful for interactive mode now"""
        return self.time_slots_taken

    def track_replicas_on_nodes(self):
        """Useful for interactive mode now"""
        pass

    def represent_current_state(self):
        """Useful for interactive mode now"""
        pass
