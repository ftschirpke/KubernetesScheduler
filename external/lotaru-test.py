from pathlib import Path
import json

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
    traces_dir: Path = Path(DEFAULT_PATH)

    experiment = EXPERIMENTS[0]
    label = LABELS[0]

    machines = ["local"]

    data = []

    for machine in machines:
        node = MACHINES[machine]
        node_id = NODES.index(node)
        path = traces_dir / node / f"results_{experiment}" / f"execution_report_{machine}.csv"
        df = pd.read_csv(path)
        df = df[df["Label"] == label]
        node_entries = df.to_dict("records")
        node_entries = list(map(transform_data_dict, node_entries))
        for i, entry in enumerate(node_entries):
            entry["id"] = i * len(NODES) + node_id
        data.extend(node_entries)

    data.sort(key=lambda x: x["id"])

    for data_point in data:
        print(json.dumps(data_point))


if __name__ == "__main__":
    main()
