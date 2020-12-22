import os
from datetime import datetime
from uuid import uuid4

from matplotlib import pyplot

from dds_simulation.conf.default import PROJECT_ROOT


def draw_extrapolation(x_vector, y_vector, partitions_number):
    pyplot.figure()
    pyplot.ylim(0,1)
    pyplot.ylabel('I(U)')

    pyplot.plot(x_vector, y_vector, color='b', linewidth=3.0)

    path = os.path.join(PROJECT_ROOT, 'results',
                        '{}-consistent-partitions-inconsistency.pdf'.format(
                            partitions_number))
    pyplot.savefig(path, format='pdf')


def draw_probability_extrapolation(x_vector, y_vector, partitions,
                                   nodes_number, average, maximum=None):
    fig = pyplot.figure()
    pyplot.ylim(0, 1)
    partitions = '+'.join((str(p) for p in partitions))
    caption = f'I({partitions}) = {round(average, 4)}'
    if maximum is not None:
        caption = f'I_max({partitions}) = {maximum}'
    pyplot.xlabel(f"Experiment iterations on a simulated datastore")
    pyplot.ylabel(f"I - probability of inconsistency")

    ax = fig.add_subplot()
    ax.text(50, 0.1, caption, fontsize=15, horizontalalignment='left', verticalalignment='bottom', multialignment='left')

    pyplot.plot(x_vector, y_vector, color='b', linewidth=4.0)
    pyplot.plot([5, len(y_vector)], [average, average], color='red')
    path = os.path.join(PROJECT_ROOT, 'results',
                        '{}-max-consistent-partitions-{}-nodes-{}.png'.format(
                            partitions, nodes_number, uuid4().hex))
    pyplot.savefig(path, dpi=300, format='png')


def draw_function(x_vector, y_vector, x_label, y_label, filename):
    pyplot.figure()

    pyplot.xlabel(x_label)
    pyplot.ylabel(y_label)
    print("X >>> ", x_vector)
    print("Y >>>> ", y_vector)

    pyplot.plot(x_vector, y_vector, 'o', color='red', linewidth=2.0)
    x_line = [x_vector[i] for i in range(0, len(x_vector))
              if x_vector[i] <= y_vector[i]]
    y_line = x_line[:]
    import time
    # pyplot.plot(x_line, y_line, '-', color='black', linewidth=2.0)
    path = os.path.join(PROJECT_ROOT, 'results',
                        f'{filename}-{int(time.time())}.png')
    pyplot.savefig(path, dpi=300, format='png')
