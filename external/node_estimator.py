import json
import math
import sys

from abc import ABC
from dataclasses import dataclass
from typing import Dict

import numpy as np


class NodeEstimator(ABC):

    def node_count(self) -> int:
        pass

    def learn(self, sample: Dict) -> None:
        pass

    def ranking(self) -> Dict:
        pass


LEARN_KEYS = set(["node", "task", "rchar", "target"])
ESTIMATE_KEYS = set(["estimate", "id"])


def main_loop(estimator: NodeEstimator) -> None:
    with open("/input/scheduler/node_estimator_input.txt", "w") as f:
        pass
    if len(sys.argv) == 2:
        seed_number = int(sys.argv[1])
        np.random.seed(seed_number)

    for line in sys.stdin:
        with open("/input/scheduler/node_estimator_input.txt", "a") as f:
            f.write(line)

        data = json.loads(line)

        keys = set(data.keys())

        if keys == LEARN_KEYS:
            estimator.learn(data)
        elif keys == ESTIMATE_KEYS:
            expected_nodes = data["estimate"]
            id = data["id"]
            output = [str(id)]

            ranking = None
            if estimator.node_count() >= expected_nodes:
                ranking = estimator.ranking()

            if ranking is None:  # estimator not able to produce a ranking yet
                output.append("NOT READY")
            else:
                output += [f"{node}={score}" for node, score in ranking.items()]

            print(";".join(output), flush=True)
        else:
            print(f"DEBUG: invalid input message: {data}", flush=True)


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

    def evaluate(self, x: float) -> float:
        return self.coef * x + self.intercept

    def avg_on_interval(self, interval: Range) -> float:
        first = self.evaluate(interval.start)
        second = self.evaluate(interval.end)
        return (first + second) / 2

    def compare_on_interval(self, other: "Line", interval: Range) -> float:
        """
        Compares two lines on the given interval.
        Returns the fraction of the average of the self line to the average of the other line.
        """
        self_avg = self.avg_on_interval(interval)
        other_avg = other.avg_on_interval(interval)
        if not math.isfinite(self_avg) or not math.isfinite(other_avg) or self_avg == 0 or other_avg == 0:
            return None
        return self_avg / other_avg
