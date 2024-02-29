import json
import sys
from pathlib import Path

import pandas as pd

DEFAULT_PATH = "/home/friedrich/bachelor/Lotaru-traces/traces"

NODES = ["local", "a1", "a2", "n1", "n2", "c2"]
MACHINES = {
    "local": "local",
    "asok01": "a1",
    "asok02": "a2",
    "n1": "n1",
    "n2": "n2",
    "c2": "c2"
}
EXPERIMENTS = ["atacseq", "bacass", "chipseq", "eager", "methylseq"]
LABELS = ["test", "train-1", "train-2"]

COLS = ["%cpu", "rss", "rchar", "wchar", "cpus",
        "read_bytes", "write_bytes",
        "vmem", "memory", "peak_rss"]
RENAMES = {
    "Task": "task",
    "Realtime": "realtime",
}


def transform_data_dict(data: dict) -> dict:
    new_data = {"node": MACHINES[data["Machine"]]}
    for k, v in RENAMES.items():
        new_data[v] = data[k]
    for col in COLS:
        new_data[col] = data[col]
    return new_data


def main() -> None:
    mode = None
    if len(sys.argv) == 2:
        mode = sys.argv[1]
    elif len(sys.argv) > 2:
        print("Usage: python3 all-data-test.py [mode]")
        sys.exit(1)

    traces_dir: Path = Path(DEFAULT_PATH)

    experiment = EXPERIMENTS[0]
    label = LABELS[0]

    data = []

    tasks = []
    for machine, node in MACHINES.items():
        node_id = NODES.index(node)
        path = traces_dir / node / f"results_{experiment}" / f"execution_report_{machine}.csv"
        df = pd.read_csv(path)
        df = df[df["Label"] == label]
        tasks.extend(df["Task"].unique())
        node_entries = df.to_dict("records")
        node_entries = list(map(transform_data_dict, node_entries))
        for i, entry in enumerate(node_entries):
            entry["id"] = i * len(NODES) + node_id
        data.extend(node_entries)

    data.sort(key=lambda x: x["id"])

    if mode == "task-only":
        data = filter(lambda x: x["task"] == tasks[0], data)
    elif mode == "node-only":
        data = filter(lambda x: x["node"] == NODES[0], data)

    for data_point in data:
        print(json.dumps(data_point))


if __name__ == "__main__":
    main()
