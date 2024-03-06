package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class OnlineTaremaScheduler extends TaremaScheduler {
    private static final String scriptPath = "external/transitive_node_estimator.py";
    private final TraceStorage traces = new TraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config) {
        this(execution, client, namespace, config, SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config,
                                 double onePointClusterScore) {
        super(execution, client, namespace, config);
        NodeEstimator estimator = new PythonNodeEstimator(scriptPath, availableNodes);
        this.nodeLabeller = new NodeLabeller(estimator, false, onePointClusterScore);
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        if (!nodeLabelsReady() || !taskIsKnown(taskName)) {
            return 0;
        }
        int nodeLabel = nodeLabeller.getLabels().get(node);
        int taskLabel = taskLabels.get(taskName);
        return Math.abs(nodeLabel - taskLabel);
    }

    @Override
    boolean taskIsKnown(String taskName) {
        return taskLabels.containsKey(taskName);
    }

    @Override
    boolean nodeLabelsReady() {
        return !nodeLabeller.getLabels().isEmpty();
    }

    @Override
    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);

        log.info("Online Tarema Scheduler: Pod {} terminated. Saving its trace...", pod.getName());
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Online Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        int traceId = traces.saveTaskTrace(task);
        log.info("Online Tarema Scheduler: Pod {} trace saved.", pod.getName());

        NodeWithAlloc node = task.getNode();
        boolean nodeLabelsChanged = recalculateNodeLabelsWithNewSample(node, task.getConfig().getTask(), traceId);
        if (nodeLabelsChanged && nodeLabelsReady()) {
            recalculateTaskLabels();
        }
    }

    private boolean recalculateNodeLabelsWithNewSample(NodeWithAlloc node, String taskName, int traceId) {
        long startTime = System.currentTimeMillis();

        long charactersRead = traces.getForId(traceId, LongField.CHARACTERS_READ);
        long realtime = traces.getForId(traceId, LongField.REALTIME);
        nodeLabeller.addDataPoint(node, taskName, charactersRead, realtime);
        boolean labelsChanged = nodeLabeller.updateLabels();

        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Node labels recalculated in {} ms.", endTime - startTime);
        return labelsChanged;
    }

    private void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        float[] groupWeights = GroupWeights.forLabels(nodeLabeller.getMaxLabel(), nodeLabeller.getLabels());
        taskLabels = TaskLabeller.taskLabels(traces, FloatField.CPU_PERCENTAGE, groupWeights);
        // TODO: cpu percentage or realtime here?

        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
    }
}
