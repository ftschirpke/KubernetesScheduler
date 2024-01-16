import pandas as pd
from sklearn.linear_model import BayesianRidge


def main() -> None:
    with open(0) as f:
        df = pd.read_csv(f)

    X = df.drop(columns=["runtime"]).values
    Y = df["runtime"].values
    print(X)
    print(Y)

    model = BayesianRidge(compute_score=True, fit_intercept=True)
    model.fit(X, Y)

    testX = [[4.0, 2.0]]  # HACK: This is a hardcoded placeholder value

    prediction = model.predict(testX, return_std=True)
    print("----")
    print(f"{prediction = }")
    print(f"{model.get_params() = }")
    print(f"{model.scores_ = }")
    print(f"{model.alpha_ = }")
    print(f"{model.lambda_ = }")


if __name__ == "__main__":
    main()
