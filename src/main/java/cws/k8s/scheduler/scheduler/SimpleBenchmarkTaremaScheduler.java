package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling but uses only one feature.
 */
public class SimpleBenchmarkTaremaScheduler extends TaremaScheduler {
    public static final LongField TARGET = LongField.REALTIME;
    private final TraceStorage traces = new TraceStorage();

    private final Map<String, Integer> nodeLabels;
    private final float[] groupWeights;
    private Map<String, Integer> taskLabels = new HashMap<>();

    private final LabelsLogger labelsLogger;

    public SimpleBenchmarkTaremaScheduler(String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config,
                                          Map<String, Double> speedEstimations) {
        this(execution, client, namespace, config, speedEstimations, SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public SimpleBenchmarkTaremaScheduler(String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config,
                                          Map<String, Double> speedEstimations,
                                          double singlePointClusterScore) {
        super(execution, client, namespace, config);
        this.labelsLogger = new LabelsLogger(config.workDir);

        for (String nodeName : speedEstimations.keySet()) {
            NodeWithAlloc node = client.getNodeByName(nodeName);
            if (node == null) {
                throw new IllegalArgumentException("Node " + nodeName + " does not exist.");
            }
            log.info("Node {} registered with speed estimation: {}", node.getName(), speedEstimations.get(nodeName));
        }

        labelsLogger.writeNodeEstimations(speedEstimations, TARGET.toString(), 0);

        nodeLabels = NodeLabeller.labelOnce(speedEstimations, true, singlePointClusterScore);
        labelsLogger.writeNodeLabels(nodeLabels, TARGET.toString(), 0);

        Function<String, Float> nodeWeight = nodeName -> GroupWeights.cpuNodeWeight(client.getNodeByName(nodeName));
        groupWeights = GroupWeights.forLabels(nodeLabels, nodeWeight);
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        String nodeName = node.getName();
        if (!taskIsKnown(taskName)) {
            // should not happen because TaremaScheduler checks for this already
            return Integer.MAX_VALUE;
        }
        Integer nodeLabel = nodeLabels.get(nodeName);
        if (nodeLabel == null) {
            // nodes without estimations are considered to be the slowest or not intended to be used
            return Integer.MAX_VALUE;
        }
        int taskLabel = taskLabels.get(taskName);
        return Math.abs(nodeLabel - taskLabel);
    }

    @Override
    int nodeSpeed(NodeWithAlloc node) {
        return nodeLabels.get(node.getName());
    }

    @Override
    boolean taskIsKnown(String taskName) {
        return taskLabels.containsKey(taskName);
    }

    @Override
    boolean nodeLabelsReady() {
        return true;
    }

    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);

        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Benchmark Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        traces.saveTaskTrace(task);

        recalculateTaskLabels();
    }

    private synchronized void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        taskLabels = TaskLabeller.logarithmicTaskLabels(traces, TARGET, groupWeights);
        labelsLogger.writeTaskLabels(taskLabels, TARGET.toString(), traces.size());

        long endTime = System.currentTimeMillis();
        log.info("Benchmark Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
    }


}
