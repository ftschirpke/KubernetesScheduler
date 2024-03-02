from . import OnlineModel

from collections import defaultdict
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import BayesianRidge

NODE_COL = "node"
TASK_COL = "task"

FEATURE_COL = "rchar"


class NodeRanking(OnlineModel):

    def __init__(self, target_feature: str):
        self.target = target_feature
        self.data: pd.DataFrame = None
        self.nodes: List[str] = []
        self.tasks: List[str] = []

        self.lines = defaultdict(lambda: defaultdict(None))
        self.ranges = defaultdict(lambda: defaultdict(None))

    def features(self):
        return [FEATURE_COL]

    def learn(self, sample: dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        node = sample[NODE_COL]
        task = sample[TASK_COL]
        if node not in self.nodes:
            self.nodes.append(node)
        if task not in self.tasks:
            self.tasks.append(task)

        model = BayesianRidge(alpha_init=1, lambda_init=0.001)
        node_data = self.data[self.data[NODE_COL] == node]
        data = node_data[node_data[TASK_COL] == task]
        if len(data) < 2:
            return
        x = data[FEATURE_COL].values.reshape(-1, 1)
        y = data[self.target].values
        model.fit(x, y)

        x_min = data[FEATURE_COL].min()
        x_max = data[FEATURE_COL].max()
        self.ranges[task][node] = Range(x_min, x_max)

        assert model.coef_.shape == (1,)
        coef = model.coef_[0]
        intercept = model.intercept_
        self.lines[task][node] = Line(coef, intercept)

    def predict(self, sample: dict) -> dict:
        task = sample[TASK_COL]
        node = sample[NODE_COL]
        line = self.lines[task][node]
        feature_val = sample[FEATURE_COL]
        return {"mu": line.evaluate(feature_val)}

    def ranking_by_task(self, task: str) -> list:
        diffs = [[None for _ in self.nodes] for _ in self.nodes]
        for i, node in enumerate(self.nodes):
            node_line = self.lines[task][node]
            diffs[i][i] = 0
            node_range = self.ranges[task][node]
            for false_j, other_node in enumerate(self.nodes[i + 1:]):
                j = i + false_j + 1

                other_node_line = self.lines[task][other_node]
                other_node_range = self.ranges[task][other_node]

                rng = node_range.intersection(other_node_range)
                diff = node_line.absolute_compare_on_interval(other_node_line, rng) if rng.width() > 0 else 0

                diffs[i][j] = diff
                diffs[j][i] = -diff

        # for line in diffs:
        #     print(line)

        wins = [sum([val > 0 for val in node_row]) for node_row in diffs]
        return sorted(zip(self.nodes, wins), key=lambda x: x[1], reverse=True)

    def diffs_by_task(self, task: str) -> list:
        ratios = [[None for _ in self.nodes] for _ in self.nodes]
        for i, node in enumerate(self.nodes):
            node_line = self.lines[task][node]
            ratios[i][i] = 1
            node_range = self.ranges[task][node]
            for false_j, other_node in enumerate(self.nodes[i + 1:]):
                j = i + false_j + 1

                other_node_line = self.lines[task][other_node]
                other_node_range = self.ranges[task][other_node]

                rng = node_range.intersection(other_node_range)
                ratio = node_line.relative_compare_on_interval(other_node_line, rng) if rng.width() > 0 else 1

                ratios[i][j] = ratio
                ratios[j][i] = 1 / ratio

        # for line in ratios:
        #     print(line)

        return np.array(ratios)

    def weights_by_task(self, task: str) -> float:
        task_data = self.data[self.data[TASK_COL] == task]
        weights = [[None for _ in self.nodes] for _ in self.nodes]
        for i, node in enumerate(self.nodes):
            weights[i][i] = 1
            node_data_count = len(task_data[task_data[NODE_COL] == node])
            for false_j, other_node in enumerate(self.nodes[i + 1:]):
                j = i + false_j + 1

                other_node_data_count = len(task_data[task_data[NODE_COL] == other_node])

                weight = (node_data_count - 1) * (other_node_data_count - 1)
                weights[i][j] = weight
                weights[j][i] = weight

        print(weights)

        return np.array(weights)


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
