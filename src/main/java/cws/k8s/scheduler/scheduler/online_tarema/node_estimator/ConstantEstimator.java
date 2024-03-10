package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import java.util.Map;

public record ConstantEstimator(Map<String, Double> estimations) implements NodeEstimator {
    public <T extends Number> void addDataPoint(String node, String taskName, long rchar, T targetValue) {
    }
}
