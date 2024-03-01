import sys
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from models import RUNTIME_COLUMN, OnlineModel, NaiveNodewiseBayes, TaskwiseBayes


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python3 all-data-test.py [model]")
        sys.exit(1)

    model_name = sys.argv[1]
    model: OnlineModel = None
    if model_name == "naive":
        model = NaiveNodewiseBayes()
    elif model_name == "taskwise":
        model = TaskwiseBayes()
    else:
        print(f"Invalid model name {model_name}")
        sys.exit(1)

    df = None

    for line in sys.stdin:
        data = json.loads(line)

        data["cpu"] = data["%cpu"] * data["cpus"]
        data["io"] = data["rchar"] + data["wchar"]
        data["cpu_per_rchar"] = data["%cpu"] * data["cpus"] / data["rchar"]
        data["mem_per_rchar"] = data["rss"] / data["rchar"]
        data["io_per_rchar"] = (data["rchar"] + data["wchar"]) / data["rchar"]
        data["realtime_per_rchar"] = data["realtime"] / data["rchar"]

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

    node_based = model_name == "naive"

    outer = "node" if node_based else "task"
    inner = "task" if node_based else "node"

    its = df[outer].unique()
    for it in its:
        outerdf = df[df[outer] == it]
        for i, feature in enumerate(model.features()):
            for inner_val in outerdf[inner].unique():
                subdf = outerdf[outerdf[inner] == inner_val]
                plt.scatter(subdf[feature], subdf[RUNTIME_COLUMN])

            # plt.scatter(df[feature], df[RUNTIME_COLUMN], color="blue")

            test_vals = np.linspace(outerdf[feature].min(), outerdf[feature].max(), 100)

            test_dict = avg_dict.copy()
            test_dict[outer] = it
            test_results = []
            for val in test_vals:
                test_dict[feature] = val
                test_results.append(model.predict(test_dict)["mu"])

            plt.plot(test_vals, test_results, color="red")

            plt.title(f"{feature} vs {RUNTIME_COLUMN} for {outer} {it}")
            plt.xlabel(feature)
            plt.ylabel(RUNTIME_COLUMN)
            plt.show()


if __name__ == "__main__":
    main()
