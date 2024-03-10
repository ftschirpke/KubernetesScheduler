package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import java.util.Map;

public interface NodeEstimator {
    <T extends Number> void addDataPoint(String node, String taskName, long rchar, T targetValue);
    Map<String, Double> estimations();

}
