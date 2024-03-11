from node_estimator import main_loop, NodeEstimator, Range, Line

from collections import defaultdict, deque
from typing import List, Dict

import numpy as np
import pandas as pd
from sklearn.linear_model import BayesianRidge

NODE = "node"
TASK = "task"

FEATURE = "rchar"
TARGET = "target"


class TransitiveNodeEstimator(NodeEstimator):
    def __init__(self):
        self.data: pd.DataFrame = None
        self.nodes: List[str] = []
        self.tasks: List[str] = []

        self.unprocessed_samples = defaultdict(dict)
        self.sample_pairs = defaultdict(dict)

        self.lines_to_update = set()

        self.lines: Dict[str, Dict[str, Line]] = defaultdict(lambda: defaultdict(lambda: None))
        self.ranges: Dict[str, Dict[str, Range]] = defaultdict(lambda: defaultdict(lambda: None))
        self.data_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.ratio_matrices_by_task: Dict[str, np.array] = {}
        self.weight_matrices_by_task: Dict[str, np.array] = {}
        self.comparison_possible: np.array = None

    def node_count(self) -> int:
        return len(self.nodes)

    def _add_node(self, node: str) -> None:
        self.nodes.append(node)
        self.ratio_matrices_by_task = {task: np.pad(mat, (0, 1)) for task, mat in self.ratio_matrices_by_task.items()}
        self.weight_matrices_by_task = {task: np.pad(mat, (0, 1)) for task, mat in self.weight_matrices_by_task.items()}
        if self.comparison_possible is None:
            self.comparison_possible = np.zeros((1, 1), dtype=bool)
        else:
            self.comparison_possible = np.pad(self.comparison_possible, (0, 1))

    def _add_task(self, task: str) -> None:
        self.tasks.append(task)
        shape = (self.node_count(), self.node_count())
        self.ratio_matrices_by_task[task] = np.zeros(shape, dtype=np.float64)
        self.weight_matrices_by_task[task] = np.zeros(shape, dtype=np.int32)

    def learn(self, sample: Dict) -> None:
        node = sample[NODE]
        task = sample[TASK]
        if self.data_counts[task][node] > 0:
            self._add_sample(sample)
        elif node in self.unprocessed_samples[task].keys():
            older_sample = self.unprocessed_samples[task].pop(node)
            if task in self.tasks:
                self._add_sample(older_sample)
                self._add_sample(sample)
            else:
                self.sample_pairs[task][node] = (older_sample, sample)
                if len(self.sample_pairs[task]) > 1:
                    any_node_already_known = any((node in self.nodes) for node in self.sample_pairs[task].keys())
                    if any_node_already_known or self.node_count() == 0:
                        # add samples if they can be compared to each other
                        for s1, s2 in self.sample_pairs.pop(task).values():
                            self._add_sample(s1)
                            self._add_sample(s2)
        else:
            self.unprocessed_samples[task][node] = sample
            return

        self._update_lines()

    def _add_sample(self, sample: Dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        node = sample[NODE]
        task = sample[TASK]
        if node not in self.nodes:
            self._add_node(node)
        if task not in self.tasks:
            self._add_task(task)

        self.data_counts[task][node] += 1
        self.lines_to_update.add((task, node))

    def _update_lines(self) -> None:
        # TODO: remove these assertions:
        assert set(self.ratio_matrices_by_task.keys()) == set(self.tasks)
        assert set(self.weight_matrices_by_task.keys()) == set(self.tasks)
        wanted_shape = (self.node_count(), self.node_count())
        for mat in self.ratio_matrices_by_task.values():
            assert mat.shape == wanted_shape
        for mat in self.weight_matrices_by_task.values():
            assert mat.shape == wanted_shape

        for task, node in self.lines_to_update:
            line_changed = self._update_line(task, node)
            if line_changed:
                self._update_ratios(task, node)
        self.lines_to_update.clear()

    def accumulated_ratios(self) -> np.array:
        shape = (self.node_count(), self.node_count())
        weighted_ratios_summed = np.zeros(shape, dtype=np.float64)
        weights_summed = np.zeros(shape, dtype=np.int32)
        for task, ratio_matrix in self.ratio_matrices_by_task.items():
            weight_matrix = self.weight_matrices_by_task[task]
            weighted_ratios_summed += np.multiply(ratio_matrix, weight_matrix)
            weights_summed += weight_matrix

        assert np.all((weights_summed != 0) == self.comparison_possible)  # TODO: check assertion (important for division below)
        adjusted_weights = weights_summed + (1 - self.comparison_possible)
        return np.divide(weighted_ratios_summed, adjusted_weights)

    def _update_line(self, task: str, node: str) -> bool:
        task_data = self.data[self.data[TASK] == task]
        data = task_data[task_data[NODE] == node]

        # TODO: remove assertion and move if statement to the beginning
        assert self.data_counts[task][node] == len(data)
        assert self.data_counts[task][node] >= 2
        # TODO: remove if statement
        # if self.data_counts[task][node] < 2:
        #     return False

        model = BayesianRidge(alpha_init=1, lambda_init=0.001)
        x = data[FEATURE].values.reshape(-1, 1)
        y = data[TARGET].values
        model.fit(x, y)

        x_min = data[FEATURE].min()
        x_max = data[FEATURE].max()
        self.ranges[task][node] = Range(x_min, x_max)

        coef = model.coef_[0]
        intercept = model.intercept_
        self.lines[task][node] = Line(coef, intercept)

        return True

    def _update_ratios(self, task: str, node: str) -> None:
        i = self.nodes.index(node)
        node_data_count = self.data_counts[task][node]
        node_line = self.lines[task][node]
        node_range = self.ranges[task][node]
        if not _is_valid_data(node_data_count, node_line, node_range):
            return

        task_ratio_matrix = self.ratio_matrices_by_task[task]
        task_weight_matrix = self.weight_matrices_by_task[task]

        for j, other_node in enumerate(self.nodes):
            if i == j:
                continue
            other_node_data_count = self.data_counts[task][other_node]
            other_node_line = self.lines[task][other_node]
            other_node_range = self.ranges[task][other_node]

            if not _is_valid_data(other_node_data_count, other_node_line, other_node_range):
                continue

            intersect_range = node_range.intersection(other_node_range)
            if intersect_range.width() <= 0:
                continue

            ratio = node_line.compare_on_interval(other_node_line, intersect_range)
            ratio = np.log(ratio)
            weight = (node_data_count - 1) * (other_node_data_count - 1)

            task_ratio_matrix[i][j] = ratio
            task_ratio_matrix[j][i] = - ratio
            task_weight_matrix[i][j] = weight
            task_weight_matrix[j][i] = weight
            self.comparison_possible[i][j] = True
            self.comparison_possible[j][i] = True

    def _floyd_warshall(self) -> (np.array, np.array):
        distances = np.array(self.comparison_possible, dtype=np.int32)
        vertice_count = distances.shape[0]
        distances[distances == 0] = vertice_count

        # TODO: remove these assertions
        assert vertice_count == self.node_count()
        assert distances.shape == (vertice_count, vertice_count)

        predecessors = np.zeros(distances.shape, dtype=np.int32)

        for start, dest in np.argwhere(distances != 0):
            predecessors[start][dest] = start

        for max_node in range(vertice_count):
            for start in range(vertice_count):
                for dest in range(vertice_count):
                    if distances[start][dest] > distances[start][max_node] + distances[max_node][dest]:
                        distances[start][dest] = distances[start][max_node] + distances[max_node][dest]
                        predecessors[start][dest] = predecessors[max_node][dest]

        return distances, predecessors

    def transitive_ratios(self) -> np.array:
        distances, predecessors = self._floyd_warshall()
        if np.any(distances >= self.node_count()):
            # not ready for ranking
            return None

        accumulated_ratios = self.accumulated_ratios()
        ratios_calculated = self.comparison_possible.copy()
        for i, _ in enumerate(self.nodes):
            ratios_calculated[i][i] = True

        todo_stack = deque()
        for start_node, dest_node in np.argwhere(ratios_calculated == False):
            start = start_node
            dest = dest_node
            while not ratios_calculated[start][dest]:
                todo_stack.append(dest)
                dest = predecessors[start][dest]
            ln_ratio = accumulated_ratios[start][dest]
            while todo_stack:
                middle = dest
                dest = todo_stack.pop()
                ln_ratio += accumulated_ratios[middle][dest]
                accumulated_ratios[start][dest] = ln_ratio
                accumulated_ratios[dest][start] = - ln_ratio
                ratios_calculated[start][dest] = True
                ratios_calculated[dest][start] = True

        # TODO: remove this assertion
        assert np.all(ratios_calculated)
        return accumulated_ratios

    def ranking(self) -> Dict:
        ratios = self.transitive_ratios()
        if ratios is None:
            return None
        ratio_sums = np.sum(ratios, axis=1) / self.node_count()
        ratio_sums = np.exp(ratio_sums)
        return dict(zip(self.nodes, ratio_sums))


def _is_valid_data(node_data_count: int, node_line: Line, node_range: Range) -> bool:
    return node_data_count >= 2 and node_line is not None and node_range is not None and node_range.width() > 0


if __name__ == "__main__":
    estimator = TransitiveNodeEstimator()
    main_loop(estimator)
