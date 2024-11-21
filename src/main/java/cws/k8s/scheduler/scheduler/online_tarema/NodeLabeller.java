package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.ConstantEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class NodeLabeller<T extends Number> {
    private final boolean higherIsBetter;
    @Getter
    private final Map<String, Integer> labels = new HashMap<>();
    private final Map<String, Map<String, Integer>> taskSpecificLabels = new HashMap<>();
    @Getter
    private Map<String, Double> estimations = new HashMap<>();
    @Getter
    private Map<String, Map<String, Double>> taskSpecificEstimations = new HashMap<>();
    private final SilhouetteScore<PointWithName<String>> silhouetteScore;
    @Getter
    private final NodeEstimator<T> estimator;

    public NodeLabeller(NodeEstimator<T> estimator, boolean higherIsBetter) {
        this(estimator, higherIsBetter, SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public Map<String, Map<String, Integer>> getAllTaskSpecificLabels() {
        return taskSpecificLabels;
    }

    public Map<String, Integer> getTaskSpecificLabels(String taskName) {
        return taskSpecificLabels.get(taskName);
    }

    public Map<String, Integer> getLabelsForTask(String taskName) {
        if (taskSpecificLabels.containsKey(taskName)) {
            return taskSpecificLabels.get(taskName);
        }
        return labels;
    }

    public NodeLabeller(NodeEstimator<T> estimator, boolean higherIsBetter, double singlePointClusterScore) {
        this.estimator = estimator;
        this.silhouetteScore = new SilhouetteScore<>(singlePointClusterScore);
        this.higherIsBetter = higherIsBetter;
    }

    public static Map<String, Integer> labelOnce(Map<String, Double> estimations, boolean higherIsBetter) {
        return labelOnce(estimations, higherIsBetter, SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public static Map<String, Integer> labelOnce(Map<String, Double> estimations,
                                                 boolean higherIsBetter,
                                                 double singlePointClusterScore) {
        ConstantEstimator<Double> estimator = new ConstantEstimator<>(estimations);
        NodeLabeller<Double> labeller = new NodeLabeller<>(estimator, higherIsBetter, singlePointClusterScore);
        labeller.updateLabels();
        return labeller.getLabels();
    }

    public Integer getMaxLabel() {
        return labels.values().stream().max(Integer::compareTo).orElse(null);
    }

    private static Map<String, Integer> calculateNewLabels(
            Map<String, Double> estimations,
            SilhouetteScore<PointWithName<String>> silhouetteScore,
            boolean higherIsBetter
    ) {
        List<PointWithName<String>> points = estimations.entrySet().stream()
                .map(entry -> new PointWithName<>(entry.getKey(), entry.getValue()))
                .toList();
        List<CentroidCluster<PointWithName<String>>> clusters = silhouetteScore.findBestKmeansClustering(points);
        if (clusters.isEmpty()) {
            return new HashMap<>();
        }
        if (clusters.size() == 1) {
            return estimations.keySet().stream()
                    .map(node -> Map.entry(node, 0))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        Comparator<CentroidCluster<PointWithName<String>>> comparator
                = Comparator.comparingDouble(cluster -> cluster.getCenter().getPoint()[0]);
        if (!higherIsBetter) {
            comparator = comparator.reversed();
        }
        clusters.sort(comparator);

        return IntStream.range(0, clusters.size())
                .boxed()
                .flatMap(i -> clusters.get(i).getPoints().stream()
                        .map(point -> Map.entry(point.getName(), i))
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void addDataPoint(String node, String taskName, long rchar, T targetValue) {
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
        NodeEstimator.NodeRankings newRankings = estimator.taskSpecificEstimations();
        Map<String, Double> newEstimations = newRankings.generalRanking();
        Map<String, Map<String, Double>> newTaskSpecific = newRankings.taskSpecificRankings();
        if (newEstimations == null) {
            if (!estimations.isEmpty()) {
                log.error("Estimator did not return new estimations; using old ones");
            }
            return false;
        }
        if (newEstimations.equals(estimations) && newTaskSpecific.equals(taskSpecificEstimations)) {
            return false;
        }
        estimations = newEstimations;
        taskSpecificEstimations = newTaskSpecific;
        return true;
    }

    private boolean recalculateLabelsFromEstimations() {
        Map<String, Integer> newGeneralLabels = calculateNewLabels(estimations, silhouetteScore, higherIsBetter);
        Map<String, Map<String, Integer>> newTaskSpecificLabels = taskSpecificEstimations.entrySet().stream()
                .map(entry -> {
                    String task = entry.getKey();
                    Map<String, Integer> labels = calculateNewLabels(entry.getValue(), silhouetteScore, higherIsBetter);
                    return Map.entry(task, labels);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        boolean labelsChanged;
        synchronized (labels) {
            synchronized (taskSpecificLabels) {
                labelsChanged = !newGeneralLabels.equals(labels) || !newTaskSpecificLabels.equals(taskSpecificLabels);
                if (!newGeneralLabels.keySet().containsAll(labels.keySet())) {
                    log.error("New node labels do not contain all nodes; lost nodes");
                    throw new IllegalStateException("New node labels do not contain all nodes; lost nodes");
                }
                labels.putAll(newGeneralLabels);
                taskSpecificLabels.putAll(newTaskSpecificLabels);
            }
        }
        return labelsChanged;
    }


}
