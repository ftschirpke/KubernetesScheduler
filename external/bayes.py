import pandas as pd
from sklearn.linear_model import BayesianRidge

RUNTIME_COLUMN = "runtime"


def main() -> None:
    with open(0) as f:
        df = pd.read_csv(f, index_col=-1, header=None).T

    X = df.drop(columns=[RUNTIME_COLUMN]).values
    Y = df[RUNTIME_COLUMN].values
    print("== X ==")
    print(X)
    print("== Y ==")
    print(Y)

    model = BayesianRidge(compute_score=True, fit_intercept=True, alpha_init=1, lambda_init=1)
    model.fit(X, Y)

    print("----")
    print(f"{model.get_params() = }")
    print(f"{model.scores_ = }")
    print(f"{model.coef_ = }")
    print(f"{model.intercept_ = }")
    print(f"{model.alpha_ = }")
    print(f"{model.lambda_ = }")


if __name__ == "__main__":
    main()
