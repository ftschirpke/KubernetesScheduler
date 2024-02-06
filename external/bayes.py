from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import BayesianRidge

RUNTIME_COLUMN = "realtime"  # TODO: Find out if this is 0 often enough to be a problem
# RUNTIME_COLUMN = "duration"  # TODO: Find out what the difference is to "realtime"
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


def bayes(df: pd.DataFrame, columns: List[str]) -> (float, float):
    X = df[columns].values
    Y = df[RUNTIME_COLUMN].values

    model = BayesianRidge(compute_score=True, fit_intercept=True, alpha_init=1, lambda_init=0.001)
    model.fit(X, Y)

    print(f"DEBUG: {model.get_params() = }")
    print(f"DEBUG: {model.scores_ = }")
    print(f"DEBUG: {model.coef_ = }")
    print(f"DEBUG: {model.intercept_ = }")
    print(f"DEBUG: {model.alpha_ = }")
    print(f"DEBUG: {model.lambda_ = }")
    print(f"DEBUG: {model.score(X, Y) = }")

    predictor_input = np.array([TEST_VALUES[col] for col in columns]).reshape(1, -1)
    mean, std = model.predict(predictor_input, return_std=True)
    assert mean.shape == (1,)
    assert std.shape == (1,)
    return mean[0], std[0]


def main() -> None:
    with open(0) as f:
        df = pd.read_csv(f, index_col=0, header=None).T

    cpu_mean, cpu_std = bayes(df, CPU_COLUMNS)
    print(f"CPU,{cpu_mean},{cpu_std}")
    mem_mean, mem_std = bayes(df, MEM_COLUMNS)
    print(f"MEM,{mem_mean},{mem_std}")
    seq_read_mean, seq_read_std = bayes(df, SEQ_READ_COLUMNS)
    print(f"SEQ_READ,{seq_read_mean},{seq_read_std}")
    seq_write_mean, seq_write_std = bayes(df, SEQ_WRITE_COLUMNS)
    print(f"SEQ_WRITE,{seq_write_mean},{seq_write_std}")


if __name__ == "__main__":
    main()
