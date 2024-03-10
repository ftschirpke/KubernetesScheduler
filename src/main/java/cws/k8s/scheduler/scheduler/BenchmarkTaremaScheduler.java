package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling approach very closely.
 */
public class BenchmarkTaremaScheduler extends TaremaScheduler {
    private static final TraceField<Float> CPU_TARGET =  FloatField.CPU_PERCENTAGE;
    private static final TraceField<Long> MEMORY_TARGET = LongField.RESIDENT_SET_SIZE;
    private static final TraceField<Long> READ_TARGET = LongField.CHARACTERS_READ;
    private static final TraceField<Long> WRITE_TARGET = LongField.CHARACTERS_WRITTEN;

    private final TraceStorage traces = new TraceStorage();

    private final NodeLabeller.LabelState cpuNodeLabelState;
    private final float[] cpuGroupWeights;
    private Map<String, Integer> cpuTaskLabels = new HashMap<>();

    private final NodeLabeller.LabelState memoryNodeLabelState;
    private final float[] memoryGroupWeights;
    private Map<String, Integer> memoryTaskLabels = new HashMap<>();

    private final NodeLabeller.LabelState readNodeLabelState;
    private final float[] readGroupWeights;
    private Map<String, Integer> readTaskLabels = new HashMap<>();

    private final NodeLabeller.LabelState writeNodeLabelState;
    private final float[] writeGroupWeights;
    private Map<String, Integer> writeTaskLabels = new HashMap<>();

    private final LabelsLogger labelsLogger;

    public BenchmarkTaremaScheduler(String execution,
                                    KubernetesClient client,
                                    String namespace,
                                    SchedulerConfig config,
                                    Map<String, Double> cpuSpeedEstimations,
                                    Map<String, Double> memorySpeedEstimations,
                                    Map<String, Double> readSpeedEstimations,
                                    Map<String, Double> writeSpeedEstimations) {
        this(execution, client, namespace, config,
                cpuSpeedEstimations, memorySpeedEstimations, readSpeedEstimations, writeSpeedEstimations,
                SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public BenchmarkTaremaScheduler(String execution,
                                    KubernetesClient client,
                                    String namespace,
                                    SchedulerConfig config,
                                    Map<String, Double> cpuSpeedEstimations,
                                    Map<String, Double> memorySpeedEstimations,
                                    Map<String, Double> readSpeedEstimations,
                                    Map<String, Double> writeSpeedEstimations,
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

        if (!cpuSpeedEstimations.keySet().equals(memorySpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(readSpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(writeSpeedEstimations.keySet())) {
            throw new IllegalArgumentException("Node estimations must be for the same nodes");
        }
        for (String nodeName : cpuSpeedEstimations.keySet()) {
            NodeWithAlloc node = client.getNodeByName(nodeName);
            if (node == null) {
                throw new IllegalArgumentException("Node " + nodeName + " does not exist.");
            }
            log.info("Node {} registered with estimations: CPU: {}, Memory: {}, Read: {}, Write: {}",
                    node.getName(),
                    cpuSpeedEstimations.get(nodeName),
                    memorySpeedEstimations.get(nodeName),
                    readSpeedEstimations.get(nodeName),
                    writeSpeedEstimations.get(nodeName)
            );
        }

        labelsLogger.writeNodeEstimations(cpuSpeedEstimations, CPU_TARGET.toString(), 0);
        labelsLogger.writeNodeEstimations(memorySpeedEstimations, MEMORY_TARGET.toString(), 0);
        labelsLogger.writeNodeEstimations(readSpeedEstimations, READ_TARGET.toString(), 0);
        labelsLogger.writeNodeEstimations(writeSpeedEstimations, WRITE_TARGET.toString(), 0);

        cpuNodeLabelState = NodeLabeller.labelOnce(cpuSpeedEstimations, true, singlePointClusterScore);
        memoryNodeLabelState = NodeLabeller.labelOnce(memorySpeedEstimations, true, singlePointClusterScore);
        readNodeLabelState = NodeLabeller.labelOnce(readSpeedEstimations, true, singlePointClusterScore);
        writeNodeLabelState = NodeLabeller.labelOnce(writeSpeedEstimations, true, singlePointClusterScore);

        labelsLogger.writeNodeLabels(cpuNodeLabelState.labels(), CPU_TARGET.toString(), 0);
        labelsLogger.writeNodeLabels(memoryNodeLabelState.labels(), MEMORY_TARGET.toString(), 0);
        labelsLogger.writeNodeLabels(readNodeLabelState.labels(), READ_TARGET.toString(), 0);
        labelsLogger.writeNodeLabels(writeNodeLabelState.labels(), WRITE_TARGET.toString(), 0);

        Function<String, Float> cpuNodeWeight = nodeName -> GroupWeights.cpuNodeWeight(client.getNodeByName(nodeName));
        Function<String, Float> memoryNodeWeight = nodeName -> GroupWeights.memoryNodeWeight(client.getNodeByName(nodeName));

        cpuGroupWeights = GroupWeights.forLabels(cpuNodeLabelState.maxLabel(), cpuNodeLabelState.labels(), cpuNodeWeight);
        memoryGroupWeights = GroupWeights.forLabels(memoryNodeLabelState.maxLabel(), memoryNodeLabelState.labels(), memoryNodeWeight);
        readGroupWeights = GroupWeights.forLabels(readNodeLabelState.maxLabel(), readNodeLabelState.labels());
        writeGroupWeights = GroupWeights.forLabels(writeNodeLabelState.maxLabel(), writeNodeLabelState.labels());
    }

    Map<NodeWithAlloc, Double> mapEstimationsToNodes(Map<String, Double> estimations) {
        return estimations.entrySet().stream()
                .map(entry -> {
                    String nodeName = entry.getKey();
                    Double estimation = entry.getValue();
                    Optional<NodeWithAlloc> node = getNodeList().stream()
                            .filter(n -> n.getName().equals(nodeName))
                            .findFirst();
                    if (node.isEmpty()) {
                        throw new IllegalArgumentException("Found node estimations for non-existing node " + nodeName + ".");
                    }
                    return Map.entry(node.get(), estimation);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        String nodeName = node.getName();
        if (!taskIsKnown(taskName)) {
            // should not happen because TaremaScheduler checks for this already
            return Integer.MAX_VALUE;
        }
        if (!cpuNodeLabelState.labels().containsKey(nodeName)) {
            // nodes without estimations are considered to be the slowest or not intended to be used
            return Integer.MAX_VALUE;
        }
        int cpuDiff = Math.abs(cpuTaskLabels.get(taskName) - cpuNodeLabelState.labels().get(nodeName));
        int memoryDiff = Math.abs(memoryTaskLabels.get(taskName) - memoryNodeLabelState.labels().get(nodeName));
        int readDiff = Math.abs(readTaskLabels.get(taskName) - readNodeLabelState.labels().get(nodeName));
        int writeDiff = Math.abs(writeTaskLabels.get(taskName) - writeNodeLabelState.labels().get(nodeName));
        return cpuDiff + memoryDiff + readDiff + writeDiff;
    }

    @Override
    int nodeSpeed(NodeWithAlloc node) {
        String nodeName = node.getName();
        return cpuNodeLabelState.labels().get(nodeName)
                + memoryNodeLabelState.labels().get(nodeName)
                + readNodeLabelState.labels().get(nodeName)
                + writeNodeLabelState.labels().get(nodeName);
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

        cpuTaskLabels = TaskLabeller.taskLabels(traces, CPU_TARGET, cpuGroupWeights);
        memoryTaskLabels = TaskLabeller.taskLabels(traces, MEMORY_TARGET, memoryGroupWeights);
        readTaskLabels = TaskLabeller.taskLabels(traces, READ_TARGET, readGroupWeights);
        writeTaskLabels = TaskLabeller.taskLabels(traces, WRITE_TARGET, writeGroupWeights);

        labelsLogger.writeTaskLabels(cpuTaskLabels, CPU_TARGET.toString(), traces.size());
        labelsLogger.writeTaskLabels(memoryTaskLabels, MEMORY_TARGET.toString(), traces.size());
        labelsLogger.writeTaskLabels(readTaskLabels, READ_TARGET.toString(), traces.size());
        labelsLogger.writeTaskLabels(writeTaskLabels, WRITE_TARGET.toString(), traces.size());

        long endTime = System.currentTimeMillis();
        log.info("Benchmark Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
    }


}
