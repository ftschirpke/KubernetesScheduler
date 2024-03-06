package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import labelling.LotaruTraces;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/*
 * This class represents the Tarema approach.
 * The Tarema approach retrieves node labels from benchmarks before the scheduling process starts.
 * At runtime, only the task labels are updated.
 */
@Slf4j
public class BenchmarkTaremaApproach implements Approach {
    int nextTaskId = 0;
    private final TraceStorage traceStorage = new TraceStorage();

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

    @Getter
    String name;

    public BenchmarkTaremaApproach(double singlePointClusterScore) {
        name = String.format("BenchmarkTaremaApproach(%f)", singlePointClusterScore);

        cpuNodeLabelState = NodeLabeller.labelOnce(LotaruTraces.cpuBenchmarks, true, singlePointClusterScore);
        memoryNodeLabelState = NodeLabeller.labelOnce(LotaruTraces.memoryBenchmarks, true, singlePointClusterScore);
        readNodeLabelState = NodeLabeller.labelOnce(LotaruTraces.readBenchmarks, true, singlePointClusterScore);
        writeNodeLabelState = NodeLabeller.labelOnce(LotaruTraces.writeBenchmarks, true, singlePointClusterScore);

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
    public void onTaskTermination(TraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;
    }

    @Override
    public void recalculate() {
        cpuTaskLabels = TaskLabeller.taskLabels(traceStorage, FloatField.CPU_PERCENTAGE, cpuGroupWeights);
        memoryTaskLabels = TaskLabeller.taskLabels(traceStorage, LongField.RESIDENT_SET_SIZE, memoryGroupWeights);
        readTaskLabels = TaskLabeller.taskLabels(traceStorage, LongField.CHARACTERS_READ, readGroupWeights);
        writeTaskLabels = TaskLabeller.taskLabels(traceStorage, LongField.CHARACTERS_WRITTEN, writeGroupWeights);
    }

    private void printNodeState(String name, NodeLabeller.LabelState state) {
        System.out.printf("%s -> ", name);
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            System.out.printf("%s(%d) ", node.getName(), state.labels().get(node));
        }
        System.out.println();
    }

    @Override
    public void printNodeLabels() {
        printNodeState("CPU  ", cpuNodeLabelState);
        printNodeState("MEM  ", memoryNodeLabelState);
        printNodeState("READ ", readNodeLabelState);
        printNodeState("WRITE", writeNodeLabelState);
    }

    private void printTaskState(String name, Map<String, Integer> state) {
        System.out.printf("%s -> ", name);
        for (String task : traceStorage.getAbstractTaskNames()) {
            System.out.printf("%s(%d) ", task, state.get(task));
        }
        System.out.println();
    }

    @Override
    public void printTaskLabels() {
        printTaskState("CPU  ", cpuTaskLabels);
        printTaskState("MEM  ", memoryTaskLabels);
        printTaskState("READ ", readTaskLabels);
        printTaskState("WRITE", writeTaskLabels);
    }
}
