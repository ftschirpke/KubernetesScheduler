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
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling approach very closely.
 */
public class BenchmarkTaremaScheduler extends TaremaScheduler {
    private final TraceStorage traces = new TraceStorage();

    private final NodeLabeller.LabelState cpuNodeLabelState;
    private final float[] cpuGroupWeights;
    private Map<String, Integer> cpuTaskLabels;

    private final NodeLabeller.LabelState memoryNodeLabelState;
    private final float[] memoryGroupWeights;
    private Map<String, Integer> memoryTaskLabels;

    private final NodeLabeller.LabelState readNodeLabelState;
    private final float[] readGroupWeights;
    private Map<String, Integer> readTaskLabels;

    private final NodeLabeller.LabelState writeNodeLabelState;
    private final float[] writeGroupWeights;
    private Map<String, Integer> writeTaskLabels;

    public BenchmarkTaremaScheduler(Map<NodeWithAlloc, Double> cpuNodeEstimations,
                                    Map<NodeWithAlloc, Double> memoryNodeEstimations,
                                    Map<NodeWithAlloc, Double> readNodeEstimations,
                                    Map<NodeWithAlloc, Double> writeNodeEstimations,
                                    String execution,
                                    KubernetesClient client,
                                    String namespace,
                                    SchedulerConfig config) {
        this(SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE,
                cpuNodeEstimations, memoryNodeEstimations, readNodeEstimations, writeNodeEstimations,
                execution, client, namespace, config);
    }

    public BenchmarkTaremaScheduler(double onePointClusterScore,
                                    Map<NodeWithAlloc, Double> cpuSpeedEstimations,
                                    Map<NodeWithAlloc, Double> memorySpeedEstimations,
                                    Map<NodeWithAlloc, Double> readSpeedEstimations,
                                    Map<NodeWithAlloc, Double> writeSpeedEstimations,
                                    String execution,
                                    KubernetesClient client,
                                    String namespace,
                                    SchedulerConfig config) {
        super(execution, client, namespace, config);

        if (!cpuSpeedEstimations.keySet().equals(memorySpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(readSpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(writeSpeedEstimations.keySet())) {
            throw new IllegalArgumentException("Node estimations must be for the same nodes");
        }
        cpuNodeLabelState = NodeLabeller.labelOnce(cpuSpeedEstimations, true, onePointClusterScore);
        memoryNodeLabelState = NodeLabeller.labelOnce(memorySpeedEstimations, true, onePointClusterScore);
        readNodeLabelState = NodeLabeller.labelOnce(readSpeedEstimations, true, onePointClusterScore);
        writeNodeLabelState = NodeLabeller.labelOnce(writeSpeedEstimations, true, onePointClusterScore);

        cpuGroupWeights = GroupWeights.forLabels(cpuNodeLabelState.maxLabel(), cpuNodeLabelState.labels());
        memoryGroupWeights = GroupWeights.forLabels(memoryNodeLabelState.maxLabel(), memoryNodeLabelState.labels());
        readGroupWeights = GroupWeights.forLabels(readNodeLabelState.maxLabel(), readNodeLabelState.labels());
        writeGroupWeights = GroupWeights.forLabels(writeNodeLabelState.maxLabel(), writeNodeLabelState.labels());

        cpuTaskLabels = new HashMap<>();
        memoryTaskLabels = new HashMap<>();
        readTaskLabels = new HashMap<>();
        writeTaskLabels = new HashMap<>();
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        if (!nodeLabelsReady() || !taskIsKnown(taskName)) {
            return 0;
        }
        int cpuDiff = Math.abs(cpuTaskLabels.get(taskName) - cpuNodeLabelState.labels().get(node));
        int memoryDiff = Math.abs(memoryTaskLabels.get(taskName) - memoryNodeLabelState.labels().get(node));
        int readDiff = Math.abs(readTaskLabels.get(taskName) - readNodeLabelState.labels().get(node));
        int writeDiff = Math.abs(writeTaskLabels.get(taskName) - writeNodeLabelState.labels().get(node));
        return cpuDiff + memoryDiff + readDiff + writeDiff;
    }

    @Override
    boolean taskIsKnown(String taskName) {
        return cpuTaskLabels.containsKey(taskName)
                && memoryTaskLabels.containsKey(taskName)
                && readTaskLabels.containsKey(taskName)
                && writeTaskLabels.containsKey(taskName);
    }

    @Override
    boolean nodeLabelsReady() {
        return true;
    }

    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);

        log.info("Benchmark Tarema Scheduler: Pod {} terminated. Saving its trace...", pod.getName());
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Benchmark Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        traces.saveTaskTrace(task);
        log.info("Benchmark Tarema Scheduler: Pod {} trace saved.", pod.getName());

        recalculateTaskLabels();
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        cpuTaskLabels = TaskLabeller.taskLabels(traces, FloatField.CPU_PERCENTAGE, cpuGroupWeights);
        memoryTaskLabels = TaskLabeller.taskLabels(traces, LongField.RESIDENT_SET_SIZE, memoryGroupWeights);
        readTaskLabels = TaskLabeller.taskLabels(traces, LongField.CHARACTERS_READ, readGroupWeights);
        writeTaskLabels = TaskLabeller.taskLabels(traces, LongField.CHARACTERS_WRITTEN, writeGroupWeights);

        long endTime = System.currentTimeMillis();
        log.info("Benchmark Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
    }


}
