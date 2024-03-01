from abc import ABC
from typing import List


RUNTIME_COLUMN = "realtime"
# RUNTIME_COLUMN = "realtime_per_rchar"


class OnlineModel(ABC):

    def learn(self, sample: dict) -> None:
        pass

    def predict(self, sample: dict) -> dict:
        pass

    def features(self) -> List[str]:
        pass
