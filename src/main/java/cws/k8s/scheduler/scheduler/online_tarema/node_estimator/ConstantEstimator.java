package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import java.util.Map;

public record ConstantEstimator<T extends Number>(Map<String, Double> estimations) implements NodeEstimator<T> {
    @Override
    public void addDataPoint(String node, String taskName, long rchar, T targetValue) {}
}
