from matplotlib import pyplot as plt
from matplotlib import animation
import networkx as nx
import random as rand


class GraphDrawer(object) :
    fig = None
    graph = None
    labels = {}
    graph_position = None

    def __init__(self, graph, position):
        self.graph = graph
        self.graph_position = position
        for i in range(0, graph.number_of_nodes()):
            self.labels[i] = i

    def initiate(self):
        self.draw_graph()

    def graph_with_failing_nodes(self):
        node_list_failed = []
        fail_list_size = rand.randrange(0, graph.number_of_nodes())

        for i in range(0, fail_list_size):
            node_list_failed.append(rand.choice(graph.nodes()))

        edge_list_failed = []
        for i in range(0, fail_list_size):
            edge_list_failed.append(rand.choice(graph.edges()))

        nx.draw_networkx_nodes(graph, position,
                               nodelist=node_list_failed,
                               node_color='r',
                               node_size=500,
                               alpha=0.8)

        nx.draw_networkx_edges(graph,position,
                               edgelist=edge_list_failed,
                               edge_color='r',
                               alpha=0.5)

        self.redraw_graph(node_list_failed, edge_list_failed)

    def animate(self, i):
        self.redraw_graph(5)

    def draw_graph(self):
        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=self.graph.nodes(),
                               node_color='g',
                               node_size=500,
                               alpha=0.8)

        nx.draw_networkx_edges(self.graph,self.graph_position,
                               edgelist=self.graph.edges(),
                               alpha=0.5)
        nx.draw_networkx_labels(graph, self.graph_position, self.labels,
                                font_size=16)

    def redraw_graph(self, highlighted_node):
        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=[highlighted_node],
                               node_color='g',
                               node_size=500,
                               alpha=0.8)
        others = self.graph.nodes().copy()
        others.remove(highlighted_node)
        nx.draw_networkx_nodes(self.graph, self.graph_position,
                               nodelist=others,
                               node_size=500,
                               alpha=0.8)

        highlighted_edges = self.graph.edges(highlighted_node)

        nx.draw_networkx_edges(self.graph, self.graph_position,
                               edgelist=highlighted_edges,
                               edge_color='g',
                               alpha=0.5)

    def draw_animation(self):
        ani = animation.FuncAnimation(self.fig, self.animate,
                                      init_func = self.initiate,
                                      interval=1000)
        ani.save('graph.mp4')


graph = nx.cubical_graph()
position = nx.circular_layout(graph)

drawer = GraphDrawer(graph, position)
drawer.draw_animation()
