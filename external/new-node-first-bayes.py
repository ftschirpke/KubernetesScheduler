import json
import math
import sys
from pathlib import Path
from typing import Dict

from river.linear_model import BayesianLinearRegression
from river.metrics import MAE
from river.proba import Gaussian

RUNTIME_COLUMN = "realtime"  # TODO: Find out if this is 0 often enough to be a problem
CPU_COLUMNS = ["cpus", "%cpu"]
MEM_COLUMNS = ["vmem", "rss"]  # TODO: Maybe "vmem" is not a good predictor and/or better ones exist
SEQ_READ_COLUMNS = ["rchar"]
SEQ_WRITE_COLUMNS = ["wchar"]

TEST_VALUES = {  # HACK: These are values are totally made up, just for testing purposes - only temporary!
    "cpus": 8,
    "%cpu": 200,
    "vmem": 4 * 1024**3,
    "rss": 3 * 1024**3,
    "rchar": 4 * 1024**3,
    "wchar": 2 * 1024**3,
}


def columns_for_resource(resource: str) -> list[str]:
    if resource == "cpu":
        return CPU_COLUMNS
    if resource == "mem":
        return MEM_COLUMNS
    if resource == "seq_read":
        return SEQ_READ_COLUMNS
    if resource == "seq_write":
        return SEQ_WRITE_COLUMNS
    raise ValueError(f"Unknown resource {resource}")


class Model:
    def __init__(self, dir: Path, resource: str):
        self.resource = resource
        self.columns = columns_for_resource(resource)

        self.model = BayesianLinearRegression()
        self.metric = MAE()

        self.test_values = {col: TEST_VALUES[col] for col in self.columns}

    def update_one(self, data_point: Dict) -> (float, float):
        x = {col: data_point[col] for col in self.columns}
        y = data_point[RUNTIME_COLUMN]

        print(f"DEBUG ({self.resource}): x = {x}, y = {y}", flush=True)

        y_pred = self.model.predict_one(x)
        self.model.learn_one(x, y)
        print(f"DEBUG ({self.resource}): {self.model._m = }", flush=True)
        self.metric.update(y, y_pred)
        print(f"DEBUG ({self.resource}): {self.metric}", flush=True)
        return self.predict_test()

    def predict_test(self) -> Gaussian:
        gaussian = self.model.predict_one(self.test_values, with_dist=True)
        print(f"DEBUG ({self.resource}): mu = {gaussian.mu}, sigma = {gaussian.sigma}", flush=True)
        if math.isnan(gaussian.sigma):
            return gaussian.mu, -1
        return gaussian.mu, gaussian.sigma


def main(debug=None) -> None:
    if debug:
        debug.write("==================================================\n")

    cpu_model = Model(dir, "cpu")
    mem_model = Model(dir, "mem")
    seq_read_model = Model(dir, "seq_read")
    seq_write_model = Model(dir, "seq_write")

    i = 0
    for line in sys.stdin:
        data = json.loads(line.strip())
        if debug:
            debug.write(f"DEBUG: {data}\n")

        i += 1

        cpu_mean, cpu_std = cpu_model.update_one(data)
        mem_mean, mem_std = mem_model.update_one(data)
        seq_read_mean, seq_read_std = seq_read_model.update_one(data)
        seq_write_mean, seq_write_std = seq_write_model.update_one(data)

        if debug:
            debug.write(f"DEBUG: iteration {i}\n")

        print(f"CPU,{cpu_mean},{cpu_std}", end=";")
        print(f"MEM,{mem_mean},{mem_std}", end=";")
        print(f"SEQ_READ,{seq_read_mean},{seq_read_std}", end=";")
        print(f"SEQ_WRITE,{seq_write_mean},{seq_write_std}", flush=True)

        # HACK: fix
        # print(f"CPU,{2**10%i},0", end=";")
        # print(f"MEM,{2**10%i},0", end=";")
        # print(f"SEQ_READ,{2**10%i},0", end=";")
        # print(f"SEQ_WRITE,{2**10%i},0", flush=True)

        if debug:
            debug.write("DEBUG: done\n")


if __name__ == "__main__":
    with open("debug.txt", "a") as f:
        main(f)
