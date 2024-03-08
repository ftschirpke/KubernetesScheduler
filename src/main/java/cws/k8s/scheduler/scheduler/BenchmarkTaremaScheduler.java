package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling approach very closely.
 */
public class BenchmarkTaremaScheduler extends TaremaScheduler {
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

        if (!cpuSpeedEstimations.keySet().equals(memorySpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(readSpeedEstimations.keySet())
                || !cpuSpeedEstimations.keySet().equals(writeSpeedEstimations.keySet())) {
            throw new IllegalArgumentException("Node estimations must be for the same nodes");
        }
        Map<NodeWithAlloc, Double> cpuEstimations = mapEstimationsToNodes(cpuSpeedEstimations);
        Map<NodeWithAlloc, Double> memoryEstimations = mapEstimationsToNodes(memorySpeedEstimations);
        Map<NodeWithAlloc, Double> readEstimations = mapEstimationsToNodes(readSpeedEstimations);
        Map<NodeWithAlloc, Double> writeEstimations = mapEstimationsToNodes(writeSpeedEstimations);

        cpuNodeLabelState = NodeLabeller.labelOnce(cpuEstimations, true, singlePointClusterScore);
        memoryNodeLabelState = NodeLabeller.labelOnce(memoryEstimations, true, singlePointClusterScore);
        readNodeLabelState = NodeLabeller.labelOnce(readEstimations, true, singlePointClusterScore);
        writeNodeLabelState = NodeLabeller.labelOnce(writeEstimations, true, singlePointClusterScore);

        cpuGroupWeights = GroupWeights.forLabels(cpuNodeLabelState.maxLabel(), cpuNodeLabelState.labels());
        memoryGroupWeights = GroupWeights.forLabels(memoryNodeLabelState.maxLabel(), memoryNodeLabelState.labels());
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
    int nodeSpeed(NodeWithAlloc node) {
        if (!nodeLabelsReady()) {
            return 0;
        }
        return cpuNodeLabelState.labels().get(node)
                + memoryNodeLabelState.labels().get(node)
                + readNodeLabelState.labels().get(node)
                + writeNodeLabelState.labels().get(node);
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
