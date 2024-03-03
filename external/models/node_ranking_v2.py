from . import OnlineModel

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import List, Dict

import numpy as np
import pandas as pd
from sklearn.linear_model import BayesianRidge

NODE_COL = "node"
TASK_COL = "task"

FEATURE_COL = "rchar"


@dataclass
class Range:
    start: float
    end: float

    def width(self):
        return self.end - self.start

    def intersection(self, other: "Range") -> "Range":
        start = max(self.start, other.start)
        end = min(self.end, other.end)
        return Range(start, end)


@dataclass
class Line:
    coef: float
    intercept: float

    def __repr__(self):
        return f"y = {self.coef}x + {self.intercept}"

    def evaluate(self, x: float) -> float:
        return self.coef * x + self.intercept

    def area_on_interval(self, interval: Range) -> float:
        first = self.evaluate(interval.start)
        second = self.evaluate(interval.end)
        return (first + second) * interval.width() / 2

    def absolute_compare_on_interval(self, other: "Line", interval: Range) -> float:
        """
        Compares two lines on the given interval. Returns the difference in area between the two lines.
        """
        self_area = self.area_on_interval(interval)
        other_area = other.area_on_interval(interval)
        return self_area - other_area

    def relative_compare_on_interval(self, other: "Line", interval: Range) -> float:
        """
        Compares two lines on the given interval. Returns the fraction of the area of the self line to the area of the other line.
        """
        self_area = self.area_on_interval(interval)
        other_area = other.area_on_interval(interval)
        return self_area / other_area


class NodeRankingV2:

    def __init__(self, target_feature: str):
        self.target = target_feature
        self.data: pd.DataFrame = None
        self.nodes: List[str] = []
        self.tasks: List[str] = []

        self.lines: Dict[str, Dict[str, Line]] = defaultdict(lambda: defaultdict(lambda: None))
        self.ranges: Dict[str, Dict[str, Range]] = defaultdict(lambda: defaultdict(lambda: None))
        self.data_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.ratio_matrices_by_task: Dict[str, np.array] = {}
        self.weight_matrices_by_task: Dict[str, np.array] = {}
        self.comparison_possible: np.array = None

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
        node_count = len(self.nodes)
        self.ratio_matrices_by_task[task] = np.zeros((node_count, node_count), dtype=np.float64)
        self.weight_matrices_by_task[task] = np.zeros((node_count, node_count), dtype=np.int32)

    def learn(self, sample: dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        node = sample[NODE_COL]
        task = sample[TASK_COL]
        if node not in self.nodes:
            self._add_node(node)
        if task not in self.tasks:
            self._add_task(task)

        self.data_counts[task][node] += 1

        # TODO: remove these assertions:
        assert set(self.ratio_matrices_by_task.keys()) == set(self.tasks)
        assert set(self.weight_matrices_by_task.keys()) == set(self.tasks)
        node_count = len(self.nodes)
        wanted_shape = (node_count, node_count)
        for mat in self.ratio_matrices_by_task.values():
            assert mat.shape == wanted_shape
        for mat in self.weight_matrices_by_task.values():
            assert mat.shape == wanted_shape

        line_changed = self._update_line(task, node)
        if line_changed:
            self._update_ratios(task, node)

    def accumulated_ratios(self) -> np.array:
        node_count = len(self.nodes)
        weighted_ratios_summed = np.zeros((node_count, node_count), dtype=np.float64)
        weights_summed = np.zeros((node_count, node_count), dtype=np.int32)
        for task, ratio_matrix in self.ratio_matrices_by_task.items():
            weight_matrix = self.weight_matrices_by_task[task]
            weighted_ratios_summed += np.multiply(ratio_matrix, weight_matrix)
            weights_summed += weight_matrix

        assert np.all((weights_summed != 0) == self.comparison_possible)  # TODO: check assertion (important for division below)
        adjusted_weights = weights_summed + (1 - self.comparison_possible)
        return np.divide(weighted_ratios_summed, adjusted_weights)

    def _update_line(self, task: str, node: str) -> bool:
        task_data = self.data[self.data[TASK_COL] == task]
        data = task_data[task_data[NODE_COL] == node]

        # TODO: remove assertion and move if statement to the beginning
        assert self.data_counts[task][node] == len(data)
        if self.data_counts[task][node] < 2:
            return False

        model = BayesianRidge(alpha_init=1, lambda_init=0.001)
        x = data[FEATURE_COL].values.reshape(-1, 1)
        y = data[self.target].values
        model.fit(x, y)

        x_min = data[FEATURE_COL].min()
        x_max = data[FEATURE_COL].max()
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

            ratio = node_line.relative_compare_on_interval(other_node_line, intersect_range)
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
        assert vertice_count == len(self.nodes)
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

    def ready_for_ranking(self) -> bool:
        distances, _ = self._floyd_warshall()
        return np.all(distances < len(self.nodes))  # every node can reach (be compared to) any other node

    def transitive_ratios(self) -> np.array:
        accumulated_ratios = self.accumulated_ratios()
        ratios_calculated = self.comparison_possible.copy()
        for i, _ in enumerate(self.nodes):
            ratios_calculated[i][i] = True

        _, predecessors = self._floyd_warshall()

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

        assert np.all(ratios_calculated)
        return accumulated_ratios

    def ranking(self) -> dict:
        ratios = self.transitive_ratios()
        ratio_sums = np.sum(ratios, axis=1) / len(self.nodes)
        return dict(zip(self.nodes, ratio_sums))


def _is_valid_data(node_data_count: int, node_line: Line, node_range: Range) -> bool:
    return node_data_count >= 2 and node_line is not None and node_range is not None and node_range.width() > 0
