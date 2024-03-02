import sys
import json

from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from models import NodeRanking
from kmeans import kmeans_on_values


def main() -> None:
    if len(sys.argv) == 1:
        target = "realtime"
        plot = False
    elif len(sys.argv) == 2:
        target = sys.argv[1]
        plot = False
    elif len(sys.argv) == 3:
        target = sys.argv[1]
        plot = sys.argv[2] == "plot"
    else:
        print("Usage: python3 all-data-test.py [target] [plot]")
        sys.exit(1)

    model: NodeRanking = NodeRanking(target)

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

    outer = "task"
    inner = "node"
    feature = "rchar"

    colors = ["red", "green", "blue", "yellow", "brown", "purple", "orange", "pink", "black"]

    its = df[outer].unique()
    task_rankings = []

    node_count = len(df[inner].unique())
    ratio_sum = np.zeros((node_count, node_count))
    ln_ratio_sum = np.zeros((node_count, node_count))
    harmonic_ratio_sum = np.zeros((node_count, node_count))

    weights_summed = np.zeros((node_count, node_count))

    for it in its:
        outerdf = df[df[outer] == it]
        test_vals = np.linspace(outerdf[feature].min(), outerdf[feature].max(), 100)

        for inner_val, color in zip(outerdf[inner].unique(), colors):
            subdf = outerdf[outerdf[inner] == inner_val]
            plt.scatter(subdf[feature], subdf[target], color=color)

        # plt.scatter(df[feature], df[RUNTIME_COLUMN], color="blue")

            test_dict = avg_dict.copy()
            test_dict[outer] = it
            test_dict[inner] = inner_val
            test_results = []
            for val in test_vals:
                test_dict[feature] = val
                test_results.append(model.predict(test_dict)["mu"])

            plt.plot(test_vals, test_results, color=color, label=inner_val)

        print(f"=== Task {it} ===")

        plt.title(f"{feature} vs {target} for {outer} {it}")
        plt.xlabel(feature)
        plt.ylabel(target)
        plt.legend()
        if plot:
            plt.show()

        weights = model.weights_by_task(it)
        weights_summed += weights

        ranking = model.ranking_by_task(it)
        print(ranking)
        task_rankings.append(ranking)

        ratios = model.diffs_by_task(it)
        ratio_sum += np.multiply(ratios - 1, weights)
        if np.any(ratios < 0):
            print(f"Negative ratio for {it}")
            print(ratios)
            plt.show()
        ln_ratio_sum += np.multiply(np.log(ratios), weights)
        harmonic_ratio_sum += 1 / np.multiply(ratios - 1, weights)

    print("=== Borda count ===")
    counts = defaultdict(int)
    for ranking in task_rankings:
        for node, rank in ranking:
            counts[node] += rank
    for node, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{node}: {count}")

    print("=== Ratio sum ===")
    print(np.divide(ratio_sum, weights_summed))
    print("=== Ratio sum averaged ===")
    ratio_avg = np.divide(ratio_sum, weights_summed) / node_count
    print(ratio_avg)
    res = - np.sum(ratio_avg, axis=1)
    print(res)
    print(res / np.linalg.norm(res))
    labels = kmeans_on_values(res.reshape(-1, 1))
    print(labels)

    print("=== Log Ratio sum ===")
    print(np.divide(ln_ratio_sum, weights_summed))
    print("=== Log Ratio sum averaged ===")
    log_ratio_avg = np.divide(ln_ratio_sum, weights_summed) / node_count
    print(log_ratio_avg)
    res = - np.sum(log_ratio_avg, axis=1)
    print(res)
    print(res / np.linalg.norm(res))
    labels = kmeans_on_values(res.reshape(-1, 1))
    print(labels)

    # print("=== Harmonic Ratio sum ===")
    # print(np.divide(harmonic_ratio_sum, node_count / weights_summed))

    print(model.nodes)
    print("cpu", [2, 0, 0, 1, 2, 3])
    print("mem", [1, 0, 0, 0, 1, 1])
    print("io ", [2, 0, 1, 3, 3, 3])


if __name__ == "__main__":
    main()
