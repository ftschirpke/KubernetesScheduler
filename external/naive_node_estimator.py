from node_estimator import main_loop, NodeEstimator, Range, Line

from collections import defaultdict
from typing import Dict, List

import numpy as np
import pandas as pd

from sklearn.linear_model import BayesianRidge

NODE = "node"
TASK = "task"
FEATURE = "rchar"
TARGET = "target"


class NaiveNodeEstimator(NodeEstimator):

    def __init__(self):
        self.data: pd.DataFrame = None
        self.nodes: List[str] = []
        self.lines: Dict[str, Line] = defaultdict(lambda: None)
        self.ranges: Dict[str, Range] = defaultdict(lambda: None)

        self.unprocessed_samples = {}
        self.lines_to_update = set()

    def node_count(self) -> int:
        return len(self.nodes)

    def learn(self, sample: Dict) -> None:
        node = sample[NODE]
        if node in self.nodes:
            self._add_sample(sample)
        elif node in self.unprocessed_samples.keys():
            unique_feature_values = set(older_sample[FEATURE] for older_sample in self.unprocessed_samples[node])
            unique_feature_values.add(sample[FEATURE])
            if len(unique_feature_values) == 1:
                self.unprocessed_samples[node].append(sample)
                return
            older_samples = self.unprocessed_samples.pop(node)
            for older_sample in older_samples:
                self._add_sample(older_sample)
            self._add_sample(sample)
        else:
            self.unprocessed_samples[node] = [sample]
            return

        for node in self.lines_to_update:
            self._update_line(node)
        self.lines_to_update.clear()

    def _add_sample(self, sample: Dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        node = sample[NODE]
        if node not in self.nodes:
            self.nodes.append(node)

        self.lines_to_update.add(node)

    def _update_line(self, node: str) -> None:
        data = self.data[self.data[NODE] == node]

        model = BayesianRidge(alpha_init=1, lambda_init=0.001)
        x = data[FEATURE].values.reshape(-1, 1)
        y = data[TARGET].values
        model.fit(x, y)

        x_min = data[FEATURE].min()
        x_max = data[FEATURE].max()
        self.ranges[node] = Range(x_min, x_max)

        coef = model.coef_[0]
        intercept = model.intercept_
        self.lines[node] = Line(coef, intercept)

    def _node_ratio(self, node: str, other_node: str) -> float:
        if node not in self.nodes or other_node not in self.nodes:
            return None

        node_range = self.ranges[node]
        other_node_range = self.ranges[other_node]
        if node_range is None or other_node_range is None:
            return None

        intersection = node_range.intersection(other_node_range)
        if intersection.width() <= 0:
            return None

        line = self.lines[node]
        other_line = self.lines[other_node]
        if line is None or other_line is None:
            return None
        return line.compare_on_interval(other_line, intersection)

    def ranking(self) -> Dict:
        shape = (self.node_count(), self.node_count())
        ln_ratios = np.zeros(shape, dtype=np.float64)
        for i, node in enumerate(self.nodes):
            for shifted_j, other_node in enumerate(self.nodes[i + 1:]):
                j = i + 1 + shifted_j
                ratio = self._node_ratio(node, other_node)
                if ratio is None:
                    return None  # not enough data to compare these nodes
                ratio = np.log(ratio)
                ln_ratios[i][j] = ratio
                ln_ratios[j][i] = -ratio

        ratio_sums = np.sum(ln_ratios, axis=1) / self.node_count()
        ratio_sums = np.exp(ratio_sums)
        return dict(zip(self.nodes, ratio_sums))


if __name__ == "__main__":
    estimator = NaiveNodeEstimator()
    main_loop(estimator)
