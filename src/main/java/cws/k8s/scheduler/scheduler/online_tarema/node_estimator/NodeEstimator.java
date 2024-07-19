package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import java.util.HashMap;
import java.util.Map;

public interface NodeEstimator<T extends Number> {
    void addDataPoint(String node, String taskName, long rchar, T targetValue);

    Map<String, Double> estimations();

    default NodeRankings taskSpecificEstimations() {
        return new NodeRankings(estimations(), new HashMap<>());
    }

    public record NodeRankings(
            Map<String, Double> generalRanking,
            Map<String, Map<String, Double>> taskSpecificRankings
    ) {
    }
}
