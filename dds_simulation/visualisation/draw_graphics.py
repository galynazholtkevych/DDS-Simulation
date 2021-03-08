import os

from matplotlib import pyplot
from dds_simulation.conf.default import PROJECT_ROOT
from dds_simulation.experiments.constants import AVAILABILITY_THRESHOLD
import time


def draw_availability_graph(writes, reads, data, alg_folder):
    filename = f'w_{writes}_r_{reads}_{str(time.time())}'
    draw_function(data, 'dataunits', 'consistent nodes', filename, alg_folder)


def draw_function(data, x_label, y_label, filename, alg_folder):
    fig = pyplot.figure(dpi=600)
    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)

    x = list(data.keys())
    values = list(data.values())

    # creating the bar plot
    pyplot.bar(x, values, color='#4285f4')
    # ax.bar(x, list(data.values()))

    pyplot.xticks([x[i] for i in range(0, len(x), len(x) // 10)])
    # draw the threshold line
    pyplot.plot([0, x[-1]], [AVAILABILITY_THRESHOLD, AVAILABILITY_THRESHOLD], color='red')
    path = os.path.join(PROJECT_ROOT, 'results', alg_folder,
                        f'{filename}.png')
    pyplot.savefig(path, dpi=600)


def draw_time_measurements(x_vector, y_vector, x_label, y_label, alg_folder, filename):
    fig = pyplot.figure(dpi=600)
    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)

    size = len(x_vector)
    if size < 10:
        print("The dataset experiment is too small to be valuable")
        return

    ticks = [i for i in range(0, size, size // 10)]

    pyplot.bar(x_vector, y_vector, color='#4285f4')
    pyplot.xticks(ticks, ticks)
    pyplot.plot(x_vector, y_vector, color='blue')

    path = os.path.join(PROJECT_ROOT, 'results', alg_folder, f'{filename}.png')
    pyplot.savefig(path, dpi=600)
