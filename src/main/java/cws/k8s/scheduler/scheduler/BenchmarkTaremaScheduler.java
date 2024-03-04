package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.online_tarema.*;
import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling approach very closely.
 */
public class BenchmarkTaremaScheduler extends TaremaScheduler {
    private final NextflowTraceStorage traces = new NextflowTraceStorage();

    private final NodeLabeller.NodeLabelState cpuNodeLabelState;
    private final float[] cpuGroupWeights;
    private Map<String, Integer> cpuTaskLabels;

    private final NodeLabeller.NodeLabelState memoryNodeLabelState;
    private final float[] memoryGroupWeights;
    private Map<String, Integer> memoryTaskLabels;

    private final NodeLabeller.NodeLabelState readNodeLabelState;
    private final float[] readGroupWeights;
    private Map<String, Integer> readTaskLabels;

    private final NodeLabeller.NodeLabelState writeNodeLabelState;
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
                                    Map<NodeWithAlloc, Double> cpuNodeEstimations,
                                    Map<NodeWithAlloc, Double> memoryNodeEstimations,
                                    Map<NodeWithAlloc, Double> readNodeEstimations,
                                    Map<NodeWithAlloc, Double> writeNodeEstimations,
                                    String execution,
                                    KubernetesClient client,
                                    String namespace,
                                    SchedulerConfig config) {
        super(execution, client, namespace, config);

        if (!cpuNodeEstimations.keySet().equals(memoryNodeEstimations.keySet())
                || !cpuNodeEstimations.keySet().equals(readNodeEstimations.keySet())
                || !cpuNodeEstimations.keySet().equals(writeNodeEstimations.keySet())) {
            throw new IllegalArgumentException("Node estimations must be for the same nodes");
        }
        cpuNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, cpuNodeEstimations);
        memoryNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, memoryNodeEstimations);
        readNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, readNodeEstimations);
        writeNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, writeNodeEstimations);

        cpuGroupWeights = GroupWeights.forLabels(cpuNodeLabelState.maxLabel(), cpuNodeLabelState.labels());
        memoryGroupWeights = GroupWeights.forLabels(memoryNodeLabelState.maxLabel(), memoryNodeLabelState.labels());
        readGroupWeights = GroupWeights.forLabels(readNodeLabelState.maxLabel(), readNodeLabelState.labels());
        writeGroupWeights = GroupWeights.forLabels(writeNodeLabelState.maxLabel(), writeNodeLabelState.labels());

        cpuTaskLabels = Map.of();
        memoryTaskLabels = Map.of();
        readTaskLabels = Map.of();
        writeTaskLabels = Map.of();
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
