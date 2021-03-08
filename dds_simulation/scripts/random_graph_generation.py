import networkx

from dds_simulation.visualisation.graph_building import draw_graph
G = networkx.watts_strogatz_graph(10, 5, 0.4)
draw_graph(G, labels=None, filename='graph-watts-strogatz-0.4-probability')
