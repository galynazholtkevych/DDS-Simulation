import os

from matplotlib import pyplot
from dds_simulation.conf.default import PROJECT_ROOT
from dds_simulation.experiments.constants import AVAILABILITY_THRESHOLD


def draw_availability_graph(writes, reads, data, alg_folder):
    filename = f'w_{writes}_r_{reads}'
    draw_function(data, 'dataunits', 'consistent nodes', filename, alg_folder)


def draw_function(data, x_label, y_label, filename, alg_folder):
    pyplot.figure(dpi=600)
    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)
    # draw the consistent nodes varying
    x = list(data.keys())
    pyplot.hist(x, list(data.values()), color='blue')
    pyplot.xticks([x[i] for i in range(0, len(x), len(x) // 10)])
    # draw the threshold line

    pyplot.plot([0, x[-1]], [AVAILABILITY_THRESHOLD, AVAILABILITY_THRESHOLD], color='red')
    path = os.path.join(PROJECT_ROOT, 'results', alg_folder,
                        f'{filename}.png')
    pyplot.savefig(path, dpi=600)


def draw_time_measurements(x_vector, y_vector, x_label, y_label, alg_folder, filename):
    pyplot.figure(dpi=600)
    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)
    pyplot.plot(x_vector, y_vector, color='blue')
    path = os.path.join(PROJECT_ROOT, 'results', alg_folder, f'{filename}.png')
    pyplot.savefig(path, dpi=600)
