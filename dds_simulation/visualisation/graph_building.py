import multiprocessing
import random
from time import time

from matplotlib import pyplot as plt
from matplotlib.animation import FuncAnimation
import networkx as nx


def generate_data():
    nodes = random.randint(5, 20)
    degree = random.randint(5, 15)
    if nodes * degree % 2 == 0 and degree < nodes:
        return nodes, degree
    return generate_data()


def draw_graph(graph, labels, filename):
    plt.figure(dpi=1200)
    nx.draw(graph, nx.circular_layout(graph),edgelist=graph.edges(data=True))
    nx.draw_networkx_labels(graph, nx.circular_layout(graph),
                            labels=labels,
                            font_size=16)
    edge_labels = nx.get_edge_attributes(graph, 'weight')
    nx.draw_networkx_edge_labels(graph, nx.circular_layout(graph),
                                 edge_labels=edge_labels)
    plt.savefig(f"{filename}.png", dpi=1200)


def form_graph(nodes_number, degree):
    return nx.random_regular_graph(degree, nodes_number)


def form_gdn(nodes, edges):
    return nx.gnm_random_graph(nodes, edges)

def form_gnp(nodes, edges):
    return nx.fast_gnp_random_graph(nodes, edges)


class Animation(object):
    """Animates graph"""

    labels = {}

    def __init__(self, queue, graph, position):
        self.queue = queue
        self.graph = graph

        self.graph_position = position
        for node in graph.nodes():
            self.labels[node] = node

        self.nodes = self.graph.nodes()
        self.edges = self.graph.edges()

    def initiate(self):
        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=self.graph.nodes(),
                               node_color='r',
                               node_size=500,
                               alpha=0.8)

        nx.draw_networkx_edges(self.graph, self.graph_position,
                               edgelist=self.graph.edges(),
                               alpha=0.5)
        nx.draw_networkx_labels(self.graph, self.graph_position, self.labels,
                                font_size=16)

    def redraw_graph(self, i):
        if not self.queue.empty():
            changed_nodes = self.queue.get_nowait()
        else:
            changed_nodes = self.graph.nodes()

        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=self.graph.nodes(),
                               node_size=500,
                               alpha=0.8)
        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=changed_nodes,
                               node_color='g',
                               node_size=500,
                               alpha=0.8)
        nx.draw_networkx_edges(self.graph, self.graph_position,
                               edgelist=self.graph.edges(),
                               edge_color='g',
                               alpha=0.5)


def plot_graph(animation):
    fig = plt.figure()

    ani = FuncAnimation(fig, animation.draw_graph,
                        init_func=animation.initiate,
                        interval=200)
    filename = f"{time()}-graph.png"
    plt.show()
    print(multiprocessing.current_process().name,"starting plot show process") #print statement preceded by true process name

    print(multiprocessing.current_process().name,"plotted graph") #print statement preceded by true process name
