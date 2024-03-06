package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import cws.k8s.scheduler.model.NodeWithAlloc;

import java.util.Map;

public interface NodeEstimator {
    <T extends Number> void addDataPoint(NodeWithAlloc node, String taskName, long rchar, T targetValue);
    Map<NodeWithAlloc, Double> estimations();

}
