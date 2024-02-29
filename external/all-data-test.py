import sys
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from models import RUNTIME_COLUMN, OnlineModel, NaiveNodewiseBayes


def main() -> None:
    model: OnlineModel = NaiveNodewiseBayes()

    df = None

    for line in sys.stdin:
        data = json.loads(line)

        if df is None:
            df = pd.DataFrame.from_dict({"row0": data}, orient="index")
        else:
            df.loc[len(df)] = data

        model.learn(data)

    # vals = [
    #     np.linspace(df[feature].min(), df[feature].max(), 100)
    #     for feature in model.features()
    # ]

    avg_dict = {feature: df[feature].mean() for feature in model.features()}

    # mesh = np.meshgrid(*vals)
    # feature_vals = [m.flatten() for m in mesh]
    # for vals in zip(*feature_vals):
    #     list_vals = list(vals)
    #     sample = dict(zip(model.features(), list_vals))

    nodes = df["node"].unique()
    for node in nodes:
        for i, feature in enumerate(model.features()):
            plt.scatter(df[feature], df[RUNTIME_COLUMN], color="blue")

            test_vals = np.linspace(df[feature].min(), df[feature].max(), 100)

            test_dict = avg_dict.copy()
            test_dict["node"] = node
            test_results = []
            for val in test_vals:
                test_dict[feature] = val
                test_results.append(model.predict(test_dict)["mu"])

            plt.plot(test_vals, test_results, color="red")

            plt.xlabel(feature)
            plt.ylabel(RUNTIME_COLUMN)
            plt.show()


if __name__ == "__main__":
    main()
