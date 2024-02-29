from . import RUNTIME_COLUMN, OnlineModel

from typing import Set, Dict

import numpy as np
import pandas as pd
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import MinMaxScaler


class NaiveNodewiseBayes(OnlineModel):

    CPU_COLUMNS = ["cpus", "%cpu"]
    MEM_COLUMNS = ["vmem", "rss"]  # TODO: Maybe "vmem" is not a good predictor and/or better ones exist
    SEQ_READ_COLUMNS = ["rchar"]
    SEQ_WRITE_COLUMNS = ["wchar"]
    # ALL_COLUMNS = CPU_COLUMNS + MEM_COLUMNS + SEQ_READ_COLUMNS + SEQ_WRITE_COLUMNS
    ALL_COLUMNS = ["rchar"]

    def __init__(self):
        self.node_models: Dict = {}
        self.data: pd.DataFrame = None
        self.x_scaler = MinMaxScaler()
        self.y_scaler = MinMaxScaler()
        self.nodes_with_new_data: Set[str] = set()

    def features(self):
        return self.ALL_COLUMNS

    def learn(self, sample: dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        node = sample["node"]
        self.nodes_with_new_data.add(node)
        if node not in self.node_models:
            self.node_models[node] = BayesianRidge(fit_intercept=False, alpha_init=0.4, lambda_init=0.001, compute_score=True)

    def predict(self, sample: dict) -> dict:
        node = sample["node"]
        model = self.node_models[node]
        if node in self.nodes_with_new_data:
            node_data = self.data[self.data["node"] == node]
            x = node_data[self.ALL_COLUMNS].values
            x = self.x_scaler.fit_transform(x)
            y = node_data[RUNTIME_COLUMN].values.ravel()
            model.fit(x, y)
            self.nodes_with_new_data.remove(node)

        sample = np.array([sample[col] for col in self.ALL_COLUMNS]).reshape(1, -1)
        sample = self.x_scaler.transform(sample)
        prediction = model.predict(sample, return_std=True)
        out = {
            "mu": prediction[0][0],
            "sigma": prediction[1][0],
            "intercept": model.intercept_,
        }
        out.update(
            {f"{col}_coef": coef for col, coef in zip(self.ALL_COLUMNS, model.coef_)}
        )
        return out


class TaskwiseBayes(OnlineModel):

    CPU_COLUMNS = ["cpus", "%cpu"]
    MEM_COLUMNS = ["vmem", "rss"]  # TODO: Maybe "vmem" is not a good predictor and/or better ones exist
    SEQ_READ_COLUMNS = ["rchar"]
    SEQ_WRITE_COLUMNS = ["wchar"]
    # ALL_COLUMNS = CPU_COLUMNS + MEM_COLUMNS + SEQ_READ_COLUMNS + SEQ_WRITE_COLUMNS
    ALL_COLUMNS = ["rchar"]

    def __init__(self):
        self.task_models: Dict = {}
        self.data: pd.DataFrame = None
        self.x_scaler = MinMaxScaler()
        self.y_scaler = MinMaxScaler()
        self.tasks_with_new_data: Set[str] = set()

    def features(self):
        return self.ALL_COLUMNS

    def learn(self, sample: dict) -> None:
        if self.data is None:
            self.data = pd.DataFrame.from_dict({"row0": sample}, orient="index")
        else:
            self.data.loc[len(self.data)] = sample

        task = sample["task"]
        self.tasks_with_new_data.add(task)
        if task not in self.task_models:
            self.task_models[task] = BayesianRidge(fit_intercept=True, alpha_init=0.4, lambda_init=0.001, compute_score=True)

    def predict(self, sample: dict) -> dict:
        task = sample["task"]
        model = self.task_models[task]
        if task in self.tasks_with_new_data:
            task_data = self.data[self.data["task"] == task]
            x = task_data[self.ALL_COLUMNS].values
            x = self.x_scaler.fit_transform(x)
            y = task_data[RUNTIME_COLUMN].values.ravel()
            model.fit(x, y, sample_weight=y)
            self.tasks_with_new_data.remove(task)

        sample = np.array([sample[col] for col in self.ALL_COLUMNS]).reshape(1, -1)
        sample = self.x_scaler.transform(sample)
        prediction = model.predict(sample, return_std=True)
        out = {
            "mu": prediction[0][0],
            "sigma": prediction[1][0],
            "intercept": model.intercept_,
        }
        out.update(
            {f"{col}_coef": coef for col, coef in zip(self.ALL_COLUMNS, model.coef_)}
        )
        return out
