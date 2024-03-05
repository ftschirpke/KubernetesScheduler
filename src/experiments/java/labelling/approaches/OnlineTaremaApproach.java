package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.*;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import labelling.LotaruTraces;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/*
 * This class represents the Online Tarema approach to labelling tasks.
 */
public class OnlineTaremaApproach<T extends Number & Comparable<T>> implements Approach {
    private int nextTaskId = 0;
    private final TraceStorage traceStorage = new TraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    private TraceField<T> target;

    static String naiveEstimator = "external/new-node-first-bayes.py";
    static String smartEstimator = "external/node-ranker.py";
    @Getter
    String name;


    public OnlineTaremaApproach(TraceField<T> target, double onePointClusterScore, String pythonScriptPath, String name) {
        this.name = name;

        NodeEstimator estimator = new PythonNodeEstimator(pythonScriptPath);
        this.nodeLabeller = new NodeLabeller(estimator, false, onePointClusterScore);
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> naive(TraceField<S> target,
                                                                                   double onePointClusterScore) {
        return new OnlineTaremaApproach<>(
                target, onePointClusterScore,
                naiveEstimator, "NaiveOnlineTaremaApproach"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> smart(TraceField<S> target,
                                                                                   double onePointClusterScore) {
        return new OnlineTaremaApproach<>(
                target, onePointClusterScore,
                smartEstimator, "SmartOnlineTaremaApproach"
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
            taskLabels = TaskLabeller.taskLabels(traceStorage, FloatField.CPU_PERCENTAGE, groupWeights);
            // TODO: cpu percentage or realtime here?
        }
    }

    @Override
    public void printNodeLabels() {
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            System.out.printf("%s : %d; ", node.getName(), nodeLabeller.getLabels().get(node));
        }
        System.out.println();
    }

    @Override
    public void printTaskLabels() {
        for (String task : traceStorage.getAbstractTaskNames()) {
            System.out.printf("%s : %d; ", task, taskLabels.get(task));
        }
        System.out.println();
    }
}
