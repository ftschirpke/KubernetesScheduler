package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.ConstantEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.CentroidCluster;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class NodeLabeller {
    @Getter
    private Integer maxLabel = null;
    @Getter
    private final Map<NodeWithAlloc, Integer> labels = new HashMap<>();
    private Map<NodeWithAlloc, Double> estimations = null;
    private final SilhouetteScore<LabelledPoint<NodeWithAlloc>> silhouetteScore;
    @Getter
    private final NodeEstimator estimator;

    public NodeLabeller(NodeEstimator estimator) {
        this(estimator, SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public NodeLabeller(NodeEstimator estimator, double onePointClusterScore) {
        this.estimator = estimator;
        this.silhouetteScore = new SilhouetteScore<>(onePointClusterScore);
    }

    public record NodeLabelState(Integer maxLabel, Map<NodeWithAlloc, Integer> labels) {
    }

    public static NodeLabeller.NodeLabelState labelOnce(Map<NodeWithAlloc, Double> estimations) {
        return labelOnce(SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE, estimations);
    }

    public static NodeLabeller.NodeLabelState labelOnce(double onePointClusterScore,
                                                        Map<NodeWithAlloc, Double> estimations) {
        ConstantEstimator estimator = new ConstantEstimator(estimations);
        NodeLabeller labeller = new NodeLabeller(estimator, onePointClusterScore);
        labeller.updateLabels();
        Integer maxLabel = labeller.getMaxLabel();
        Map<NodeWithAlloc, Integer> labels = labeller.getLabels();
        return new NodeLabeller.NodeLabelState(maxLabel, labels);
    }

    private Map<NodeWithAlloc, Integer> calculateNewLabels() {
        List<LabelledPoint<NodeWithAlloc>> points = estimations.entrySet().stream()
                .map(entry -> new LabelledPoint<>(entry.getKey(), entry.getValue()))
                .toList();
        List<CentroidCluster<LabelledPoint<NodeWithAlloc>>> clusters = silhouetteScore.findBestKmeansClustering(points);
        if (clusters.isEmpty()) {
            return new HashMap<>();
        }
        if (clusters.size() == 1) {
            return estimations.keySet().stream()
                    .map(node -> Map.entry(node, 0))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        clusters.sort(Comparator.comparingDouble(cluster -> cluster.getCenter().getPoint()[0]));
        return IntStream.range(0, clusters.size())
                .boxed()
                .flatMap(i -> clusters.get(i).getPoints().stream()
                        .map(point -> Map.entry(point.getLabel(), i))
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public <T extends Number> void addDataPoint(NodeWithAlloc node, String taskName, long rchar, T targetValue) {
        estimator.addDataPoint(node, taskName, rchar, targetValue);
    }

    public boolean updateLabels() {
        boolean estimationsChanged = retrieveNewEstimations();
        if (!estimationsChanged) {
            return false;
        }
        return recalculateLabelsFromEstimations();
    }

    private boolean retrieveNewEstimations() {
        Map<NodeWithAlloc, Double> newEstimations = estimator.estimations();
        if (newEstimations == null) {
            if (estimations != null) {
                log.error("Estimator did not return new estimations; using old ones");
            }
            return false;
        }
        if (newEstimations.equals(estimations)) {
            return false;
        }
        estimations = newEstimations;
        return true;
    }

    private boolean recalculateLabelsFromEstimations() {
        Map<NodeWithAlloc, Integer> newLabels = calculateNewLabels();
        Integer newMaxLabel = newLabels.values().stream().max(Integer::compareTo).orElse(null);
        boolean labelsChanged;
        synchronized (labels) {
            labelsChanged = !newLabels.equals(labels);
            if (!newLabels.keySet().containsAll(labels.keySet())) {
                log.error("New node labels do not contain all nodes; lost nodes");
                labels.clear();
            }
            labels.putAll(newLabels);
            maxLabel = newMaxLabel;
        }
        return labelsChanged;
    }


}
