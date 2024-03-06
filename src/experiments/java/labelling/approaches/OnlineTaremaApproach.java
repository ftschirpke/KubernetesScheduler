package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.ConstantEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import labelling.LotaruTraces;
import lombok.Getter;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;

import java.util.*;

/*
 * This class represents the Online Tarema approach to labelling tasks.
 */
public class OnlineTaremaApproach<T extends Number & Comparable<T>> implements Approach {
    private int nextTaskId = 0;
    private final TraceStorage traceStorage = new TraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    @Getter
    String name;
    private final TraceField<T> target;
    private final SilhouetteScore<DoublePoint> silhouetteScore;

    static String naiveEstimatorPath = "external/naive_node_estimator.py";
    static String smartEstimatorPath = "external/transitive_node_estimator.py";


    public OnlineTaremaApproach(TraceField<T> target,
                                boolean higherIsBetter,
                                double onePointClusterScore,
                                NodeEstimator estimator,
                                String name) {
        this.name = name;
        this.target = target;
        this.silhouetteScore = new SilhouetteScore<>(onePointClusterScore);

        this.nodeLabeller = new NodeLabeller(estimator, higherIsBetter, onePointClusterScore);
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> naive(TraceField<S> target,
                                                                                   boolean higherIsBetter,
                                                                                   double onePointClusterScore) {
        NodeEstimator naiveEstimator = new PythonNodeEstimator(naiveEstimatorPath, Set.of(LotaruTraces.nodes));
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, onePointClusterScore,
                naiveEstimator, "NaiveOnlineTaremaApproach"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> smart(TraceField<S> target,
                                                                                   boolean higherIsBetter,
                                                                                   double onePointClusterScore) {
        NodeEstimator smartEstimator = new PythonNodeEstimator(smartEstimatorPath, Set.of(LotaruTraces.nodes));
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, onePointClusterScore,
                smartEstimator, "SmartOnlineTaremaApproach"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> tarema(TraceField<S> target,
                                                                                    Map<NodeWithAlloc, Double> estimations,
                                                                                    boolean higherIsBetter,
                                                                                    double onePointClusterScore) {
        NodeEstimator constantEstimator = new ConstantEstimator(estimations);
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, onePointClusterScore,
                constantEstimator, "SimplifiedTaremaApproach"
        );
    }

    @Override
    public void onTaskTermination(TraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        int id = traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        long charactersRead = traceStorage.getForId(id, LongField.CHARACTERS_READ);
        long realtime = traceStorage.getForId(id, LongField.REALTIME);
        nodeLabeller.addDataPoint(node, config.getTask(), charactersRead, realtime);
    }

    @Override
    public void recalculate() {
        boolean labelsChanged = nodeLabeller.updateLabels();

        if (labelsChanged && !nodeLabeller.getLabels().isEmpty()) {
            float[] groupWeights = GroupWeights.forLabels(nodeLabeller.getMaxLabel(), nodeLabeller.getLabels());
            // HACK: to change the target field, change the Field here
            taskLabels = TaskLabeller.taskLabels(traceStorage, target, groupWeights);
        }
    }

    @Override
    public void printNodeLabels() {
        List<Cluster<DoublePoint>> clusters = new ArrayList<>();
        for (int i = 0; i <= nodeLabeller.getMaxLabel(); i++) {
            clusters.add(new Cluster<>());
        }
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            int label = nodeLabeller.getLabels().get(node);
            System.out.printf("%s(%d) ", node.getName(), label);
            double value = LotaruTraces.cpuBenchmarks.get(node);
            DoublePoint point = new DoublePoint(new double[]{value});
            clusters.get(label).addPoint(point);
        }
        double score = silhouetteScore.score(clusters);
        System.out.printf(" -> Silhouette score: %f\n", score);
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            Double estimation = nodeLabeller.getEstimations().get(node);
            System.out.printf("%s(%f) ", node.getName(), estimation);
        }
        System.out.println();
    }

    @Override
    public void printTaskLabels() {
        for (String task : traceStorage.getAbstractTaskNames()) {
            System.out.printf("%s(%d) ", task, taskLabels.get(task));
        }
        System.out.println();
    }
}
