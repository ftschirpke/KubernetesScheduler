import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


# apply kmeans-clustering to a single column of the input dataframe and return the labels
def kmeans(df: pd.DataFrame, col: str) -> np.ndarray:
    values = df[col].values.reshape(-1, 1)

    best_score = -1
    best_kmeans = None
    best_labels = None
    for k in range(2, values.shape[0]):
        kmeans = KMeans(n_clusters=k, n_init="auto", random_state=0)
        labels = kmeans.fit_predict(values)
        score = silhouette_score(values, labels)
        if score > best_score:
            best_score = score
            best_kmeans = kmeans
            best_labels = labels

    if best_labels is None:
        raise ValueError(f"Could not find best labels for {col = }")

    # rename labels to be in order of increasing cluster center
    cluster_centers = best_kmeans.cluster_centers_.flatten()
    sorted_labels = np.argsort(cluster_centers)
    new_labels = np.empty_like(sorted_labels)
    new_labels[sorted_labels] = np.arange(sorted_labels.size)
    return new_labels[best_labels]


# apply kmeans-clustering to each column of the input dataframe
def main() -> None:
    with open(0) as f:
        df = pd.read_csv(f, index_col=0, header=None)

    labels = pd.DataFrame(index=df.index)
    for col in df.columns:
        labels[col] = kmeans(df, col)

    print(labels.to_csv(index=True, header=False))


if __name__ == "__main__":
    main()
