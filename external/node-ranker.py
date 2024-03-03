import json
import sys

import pandas as pd

from models import NodeRankingV2


def main() -> None:
    df: pd.DataFrame = None
    model = NodeRankingV2("realtime")

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

        if len(model.nodes) < 6 or not model.ready_for_ranking():
            continue

        print(model.ranking())


if __name__ == "__main__":
    main()
