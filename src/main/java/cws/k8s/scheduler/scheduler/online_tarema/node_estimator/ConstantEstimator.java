package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import cws.k8s.scheduler.model.NodeWithAlloc;

import java.util.Map;
import java.util.Set;

public record ConstantEstimator(Map<NodeWithAlloc, Double> estimations) implements NodeEstimator {

    public <T extends Number> void addDataPoint(NodeWithAlloc node, String taskName, long rchar, T targetValue) {
    }

    public Set<NodeWithAlloc> getNodes() {
        return estimations.keySet();
    }


}
