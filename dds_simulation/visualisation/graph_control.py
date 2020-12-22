from multiprocessing import Process
from multiprocessing import Queue

import networkx as nx

from dds_simulation.visualisation.extrapolation import draw_function
from dds_simulation.visualisation.graph_building import Animation, draw_graph
from dds_simulation.visualisation.graph_building import plot_graph


class GraphController(object):
    """Class emitting the signals to dredraw links and nodes

    When links are transmitting and nodes obtain message, they
    should change the color. This class controls graph animation thread
    """

    def __init__(self, graph):

        self.graph = graph

    def animate(self):

        self.queue = Queue()
        import random
        random_node = random.randint(1, self.graph.number_of_nodes())
        self.queue.put_nowait([random_node,])

        animation = Animation(self.queue, self.graph, nx.circular_layout(
            self.graph))
        animator = Process(target=plot_graph, name="Graph Animator",
                           args=(animation,))
        animator.start()

    def message_broadcast(self, nodes, links):

        self.queue.put_nowait(nodes)

    def draw_graph(self, graph, labels, filename):
        draw_graph(graph, labels, filename)

    def draw_graphics(self, *args, **kwargs):
        draw_function(*args, **kwargs)
