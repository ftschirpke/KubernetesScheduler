from . import OnlineModel

from collections import defaultdict
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

        self.lines: Dict[str, Dict[str, Line]] = defaultdict(lambda: defaultdict(None))
        self.ranges: Dict[str, Dict[str, Range]] = defaultdict(lambda: defaultdict(None))
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
        for task, ratio_matrix in self.ratio_matrices_by_task:
            weight_matrix = self.weight_matrices_by_task[task]
            weighted_ratios_summed += np.multiply(ratio_matrix, weight_matrix)
            weights_summed += weight_matrix
        return np.divide(weighted_ratios_summed, weights_summed)  # TODO: fix division by zero

    def _update_line(self, task: str, node: str) -> bool:
        task_data = self.data[self.data[TASK_COL] == task]
        data = self.data[task_data[NODE_COL] == node]

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
            if intersect_range <= 0:
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

    def transitive_closure(self, with_ratios: bool = True) -> np.array | (np.array, np.array):
        ...  # TODO: implement floyd warshall


def _is_valid_data(node_data_count: int, node_line: Line, node_range: Range) -> bool:
    return node_data_count >= 2 and node_line is not None and node_range is not None and node_range.width() > 0
