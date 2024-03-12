package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.*;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class OnlineTaremaScheduler extends TaremaScheduler {
    private static final String SCRIPT_PATH = "external/transitive_node_estimator.py";
    private static final LongField TARGET = LongField.REALTIME;

    private final TraceStorage traces = new TraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    private final LabelsLogger labelsLogger;

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
                                 double singlePointClusterScore) {
        super(execution, client, namespace, config);
        if (config.workDir == null) {
            String workDir;
            if (config.volumeClaims.isEmpty()) {
                workDir = "/tmp/scheduler";
            } else {
                workDir = config.volumeClaims.get(0).mountPath + "/scheduler";
            }
            log.info("Work directory not set. Using default: {}", workDir);
            this.labelsLogger = new LabelsLogger(workDir);
        } else {
            this.labelsLogger = new LabelsLogger(config.workDir);
        }

        NodeEstimator estimator = new PythonNodeEstimator(SCRIPT_PATH, usedNodes);
        this.nodeLabeller = new NodeLabeller(estimator, false, singlePointClusterScore);
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        String nodeName = node.getName();
        if (!nodeLabelsReady() || !taskIsKnown(taskName)) {
            // should not happen because TaremaScheduler checks for this already
            return Integer.MAX_VALUE;
        }
        Integer nodeLabel = nodeLabeller.getLabels().get(nodeName);
        if (nodeLabel == null) {
            // prioritize nodes we don't know about yet
            return 0;
        }
        int taskLabel = taskLabels.get(taskName);
        return Math.abs(nodeLabel - taskLabel);
    }

    @Override
    int nodeSpeed(NodeWithAlloc node) {
        String nodeName = node.getName();
        if (!nodeLabelsReady()) {
            return 0;
        }
        return nodeLabeller.getLabels().get(nodeName);
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

        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        Optional<Integer> optionalTraceId = traces.saveTaskTrace(task);
        if (optionalTraceId.isEmpty()) {
            return;
        }
        log.info("Saved traces for pod {}.", pod.getName());
        int traceId = optionalTraceId.get();

        NodeWithAlloc node = task.getNode();
        recalculateNodeLabelsWithNewSample(node.getName(), task.getConfig().getTask(), traceId);
        if (nodeLabelsReady()) {
            recalculateTaskLabels();
        }
    }

    private void recalculateNodeLabelsWithNewSample(String nodeName, String taskName, int traceId) {
        long startTime = System.currentTimeMillis();

        long charactersRead = traces.getForId(traceId, LongField.CHARACTERS_READ);
        long targetValue = traces.getForId(traceId, TARGET);
        nodeLabeller.addDataPoint(nodeName, taskName, charactersRead, targetValue);
        boolean labelsChanged = nodeLabeller.updateLabels();

        if (!nodeLabeller.getEstimations().isEmpty()) {
            labelsLogger.writeNodeEstimations(nodeLabeller.getEstimations(), TARGET.toString(), traces.size());
        }
        if (labelsChanged) {
            log.info("New Node Labels calculated: {}", nodeLabeller.getLabels());
            labelsLogger.writeNodeLabels(nodeLabeller.getLabels(), TARGET.toString(), traces.size());
        }

        long endTime = System.currentTimeMillis();
        log.info("Node labels recalculated in {} ms.", endTime - startTime);
    }

    private synchronized void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        Function<String, Float> nodeWeight = nodeName -> GroupWeights.cpuNodeWeight(client.getNodeByName(nodeName));
        float[] groupWeights = GroupWeights.forLabels(nodeLabeller.getLabels(), nodeWeight);
        taskLabels = TaskLabeller.logarithmicTaskLabels(traces, TARGET, groupWeights);
        labelsLogger.writeTaskLabels(taskLabels, TARGET.toString(), traces.size());

        long endTime = System.currentTimeMillis();
        log.info("Task labels recalculated in {} ms.", endTime - startTime);
    }
}
