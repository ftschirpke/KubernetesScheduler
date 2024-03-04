package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.LotaruTraces;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/*
 * This class represents the Online Tarema approach to labelling tasks.
 */
public class OnlineTaremaApproach implements Approach {
    private int nextTaskId = 0;
    private final NextflowTraceStorage traceStorage = new NextflowTraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    static String naiveEstimator = "external/new-node-first-bayes.py";
    static String smartEstimator = "external/node-ranker.py";
    @Getter
    String name;


    public OnlineTaremaApproach(double onePointClusterScore, String pythonScriptPath, String name) {
        this.name = name;

        NodeEstimator estimator = new PythonNodeEstimator(pythonScriptPath);
        this.nodeLabeller = new NodeLabeller(estimator, onePointClusterScore);
    }

    public static OnlineTaremaApproach naive(double onePointClusterScore) {
        return new OnlineTaremaApproach(onePointClusterScore, naiveEstimator, "NaiveOnlineTaremaApproach");
    }

    public static OnlineTaremaApproach smart(double onePointClusterScore) {
        return new OnlineTaremaApproach(onePointClusterScore, smartEstimator, "SmartOnlineTaremaApproach");
    }

    @Override
    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
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
