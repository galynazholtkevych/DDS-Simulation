from copy import deepcopy
from itertools import combinations_with_replacement
import random

from .simulation import DDS
from dds_simulation.visualisation import extrapolation
from dds_simulation.conf import default


class ConsistentExperiment(DDS):
    """

    """
    def _form_random_partitions(self):
        partitions_number = int(default.parameter('experiment', 'partitions'))
        consistent_partitions = []
        nodes = set([node.identity for node in self.nodes])

        for i in range(0, partitions_number - 1):
            nodes_for_partition = set(random.sample(
                nodes,
                random.randint(1, len(nodes))
            ))
            consistent_partitions.append(nodes_for_partition)
            nodes -= nodes_for_partition

        # NOTE(galyna): the last partition will be just the remained nodes
        consistent_partitions.append(nodes)
        return consistent_partitions

    def _form_partitions(self, partition):

        consistent_partitions = []
        nodes = set([node.identity for node in self.nodes])

        for nodes_number in partition:
            nodes_for_partition = set(random.sample(
                nodes,
                nodes_number)
            )
            consistent_partitions.append(nodes_for_partition)
            nodes -= nodes_for_partition

        # NOTE(galyna): the last partition will be just the remained nodes
        if nodes:
            consistent_partitions.append(nodes)
        return consistent_partitions

    def single_iteration(self, partition, nodes_original):
        """
         Experimental method

         Calculate the inconsistency state by calculating number of compare
         operations to find two consistent nodes.

         :return degree of DDS inconsistency
        """
        i = 0
        consistent_partitions = self._form_partitions(partition)
        taking_count = 2
        nodes = deepcopy(nodes_original)
        random.shuffle(nodes)

        taken = set(random.sample(nodes, taking_count))

        if any(taken.issubset(partition) for partition in consistent_partitions):
            return 0

        return 1

    def multiple_partitions(self, partitions_number):
        experiment_number = int(default.parameter('experiment', 'experiments'))

        nodes = [node.identity for node in self.nodes]

        partitions_invariants = [i + 1 for i in range(len(nodes))]
        print("part inv>>> ", partitions_invariants)
        partitions = list(
            filter(lambda partition: sum(partition) == len(nodes),
                   combinations_with_replacement(
                       partitions_invariants, partitions_number)))

        y = []
        x = []
        inconsistency_array = []
        print("patritions: ", partitions)
        for part in partitions:
            print("=========================================")
            print("PARTITION: ", part)

            for i in range(experiment_number):
                for j in range(experiment_number):
                    taking_result = self.single_iteration(part, nodes)
                    inconsistency_array.append(taking_result)


                probability = sum(inconsistency_array) / len(
                    inconsistency_array)

                inconsistency_array.clear()
                y.append(probability)
                x.append(i)

            print("=========================================")
            print("AVERAGE =================================")

            # probability of inconsistency:
            average = sum(y) / len(y)
            maximum = max(y)
            print(average)
            print(maximum)
            extrapolation.draw_probability_extrapolation(
                x, y, part, len(nodes), average)
            x.clear()
            y.clear()

        return x, y

    def start_experiment(self):
        partitions = int(default.parameter('experiment', 'partitions'))

        inconsistency_states = self.multiple_partitions(partitions)
        return inconsistency_states
