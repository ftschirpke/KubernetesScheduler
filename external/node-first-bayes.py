import pickle
import sys
from pathlib import Path
from typing import Dict

import pandas as pd
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


def columns_for_type(type: str) -> list[str]:
    if type == "cpu":
        return CPU_COLUMNS
    if type == "mem":
        return MEM_COLUMNS
    if type == "seq_read":
        return SEQ_READ_COLUMNS
    if type == "seq_write":
        return SEQ_WRITE_COLUMNS
    raise ValueError(f"Unknown type {type}")


def model_path(dir: Path, name: str) -> Path:
    return dir / f"node-first-{name}-bayes.pkl"


def metric_path(dir: Path, name: str) -> Path:
    return dir / f"node-first-{name}-bayes-metric.pkl"


class Model:
    def __init__(self, dir: Path, type: str):
        self.type = type
        self.columns = columns_for_type(type)

        self.model_path = model_path(dir, type)
        self._load_model()

        self.test_values = {col: TEST_VALUES[col] for col in self.columns}
        self.metric = MAE()

    def __del__(self):
        self._save_model()

    def update_one(self, data_point: Dict) -> (float, float):
        x = {col: data_point[col] for col in self.columns}
        y = data_point[RUNTIME_COLUMN]

        y_pred = self.model.predict_one(x)
        self.model.learn_one(x, y)
        self.metric.update(y, y_pred)
        print(f"DEBUG ({self.type}): {self.metric}")
        return self.predict_test()

    def predict_test(self) -> Gaussian:
        gaussian = self.model.predict_one(self.test_values, with_dist=True)
        print(f"DEBUG ({self.type}): mu = {gaussian.mu}, sigma = {gaussian.sigma}")
        return gaussian.mu, gaussian.sigma

    def _load_model(self) -> None:
        if self.model_path.exists():
            with open(self.model_path, "rb") as f:
                print(f"DEBUG: Loading model from {self.model_path}")
                self.model = pickle.load(f)
                print(f"DEBUG: Model: {self.model}")
        else:
            print("DEBUG: Creating new model")
            self.model = BayesianLinearRegression(alpha=1, beta=0.001)

    def _save_model(self) -> None:
        with open(self.model_path, "wb") as f:
            pickle.dump(self.model, f)


def main() -> None:
    if len(sys.argv) > 2:
        print("Usage: <data> | python node-first-bayes.py <optional: model-dir>")
        sys.exit(1)
    elif len(sys.argv) == 2:
        print(f"DEBUG: {sys.argv = }")
        print(f"DEBUG: Using directory {sys.argv[1]}")
        dir = Path(sys.argv[1])
        if not dir.exists():
            print(f"DEBUG: Creating directory {dir}")
            dir.mkdir(parents=True, exist_ok=True)
    else:
        dir = Path.cwd()

    with open("debug.txt", "a") as f:
        f.write("==================================================\n")

    cpu_model = Model(dir, "cpu")
    mem_model = Model(dir, "mem")
    seq_read_model = Model(dir, "seq_read")
    seq_write_model = Model(dir, "seq_write")

    with open(0) as f:
        df = pd.read_csv(f, index_col=0, header=None).T

    data = df.to_dict("index")
    for key, value in data.items():
        with open("debug.txt", "a") as f:
            f.write(f"DEBUG: iteration {key} {dir}\n")
        cpu_mean, cpu_std = cpu_model.update_one(value)
        mem_mean, mem_std = mem_model.update_one(value)
        seq_read_mean, seq_read_std = seq_read_model.update_one(value)
        seq_write_mean, seq_write_std = seq_write_model.update_one(value)

    with open("debug.txt", "a") as f:
        f.write(f"kinda done {dir}\n")

    print(f"CPU,{cpu_mean},{cpu_std}")
    print(f"MEM,{mem_mean},{mem_std}")
    print(f"SEQ_READ,{seq_read_mean},{seq_read_std}")
    print(f"SEQ_WRITE,{seq_write_mean},{seq_write_std}")

    with open("debug.txt", "a") as f:
        f.write(f"completely done {dir}\n")


if __name__ == "__main__":
    main()
