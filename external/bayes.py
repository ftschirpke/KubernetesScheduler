import pandas as pd
from sklearn.linear_model import BayesianRidge


def main() -> None:
    with open(0) as f:
        df = pd.read_csv(f, index_col=-1, header=None).T

    X = df.drop(columns=["runtime"]).values
    Y = df["runtime"].values
    print(X)
    print(Y)

    model = BayesianRidge(compute_score=True, fit_intercept=True)
    model.fit(X, Y)

    print("----")
    print(f"{model.get_params() = }")
    print(f"{model.scores_ = }")
    print(f"{model.alpha_ = }")
    print(f"{model.lambda_ = }")


if __name__ == "__main__":
    main()
