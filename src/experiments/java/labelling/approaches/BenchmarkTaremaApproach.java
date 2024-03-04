package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.LotaruTraces;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/*
 * This class represents the Tarema approach.
 * The Tarema approach retrieves node labels from benchmarks before the scheduling process starts.
 * At runtime, only the task labels are updated.
 */
@Slf4j
public class BenchmarkTaremaApproach implements Approach {
    int nextTaskId = 0;
    private final NextflowTraceStorage traceStorage = new NextflowTraceStorage();

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

    @Getter
    String name;

    public BenchmarkTaremaApproach(double onePointClusterScore) {
        name = String.format("BenchmarkTaremaApproach(%f)", onePointClusterScore);

        cpuNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, LotaruTraces.lotaruCpuBenchmarkResults);
        memoryNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, LotaruTraces.lotaruMemoryBenchmarkResults);
        readNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, LotaruTraces.lotaruReadBenchmarkResults);
        writeNodeLabelState = NodeLabeller.labelOnce(onePointClusterScore, LotaruTraces.lotaruWriteBenchmarkResults);

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
    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
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

    @Override
    public void printNodeLabels() {
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            System.out.printf("%s : [cpu: %d, memory: %d, read: %d, write: %d]; ",
                    node.getName(),
                    cpuNodeLabelState.labels().get(node),
                    memoryNodeLabelState.labels().get(node),
                    readNodeLabelState.labels().get(node),
                    writeNodeLabelState.labels().get(node));
        }
        System.out.println();
    }

    @Override
    public void printTaskLabels() {
        for (String task : traceStorage.getAbstractTaskNames()) {
            System.out.printf("%s : [cpu: %d, memory: %d, read: %d, write: %d]; ",
                    task,
                    cpuTaskLabels.get(task),
                    memoryTaskLabels.get(task),
                    readTaskLabels.get(task),
                    writeTaskLabels.get(task));
        }
        System.out.println();
    }
}
