package labelling.approaches;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import labelling.LotaruTraces;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.List;
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
    static String shortName = "bench";

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
    public void onTaskTermination(TraceRecord trace, TaskConfig config, String nodeName) {
        traceStorage.saveTrace(trace, nextTaskId, config, nodeName);
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
        for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
            System.out.printf("%s(%d) ", nodeName, state.labels().get(nodeName));
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

    @Override
    public void writeState(String experimentDir, List<String> taskNames) {
        File dir = new File(String.format("%s/%s", experimentDir, shortName));
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                log.error("Could not create directory {}", dir);
                System.exit(1);
            }
        }
        File nodeState = new File(String.format("%s/%s/constant_node_state.txt", experimentDir, shortName));
        boolean newlyCreated;
        try {
            newlyCreated = nodeState.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (newlyCreated) {
            try (PrintWriter writer = new PrintWriter(nodeState)) {
                writer.printf("CPU:   %d -> ", cpuNodeLabelState.maxLabel());
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s(%d) ", nodeName, cpuNodeLabelState.labels().get(nodeName));
                }
                writer.println();
                writer.printf("MEM:   %d -> ", cpuNodeLabelState.maxLabel());
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s(%d) ", nodeName, cpuNodeLabelState.labels().get(nodeName));
                }
                writer.println();
                writer.printf("READ:  %d -> ", cpuNodeLabelState.maxLabel());
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s(%d) ", nodeName, cpuNodeLabelState.labels().get(nodeName));
                }
                writer.println();
                writer.printf("WRITE: %d -> ", cpuNodeLabelState.maxLabel());
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s(%d) ", nodeName, cpuNodeLabelState.labels().get(nodeName));
                }
                writer.println();
            } catch (FileNotFoundException e) {
                log.error("File {} does not exist", nodeState);
                System.exit(1);
            }
        }

        File taskState = new File(String.format("%s/%s/cpu_task_labels.csv", experimentDir, shortName));
        writeTaskLabelState(nodeState, taskState, cpuTaskLabels, taskNames);

        File memTaskState = new File(String.format("%s/%s/mem_task_labels.csv", experimentDir, shortName));
        writeTaskLabelState(nodeState, memTaskState, memoryTaskLabels, taskNames);

        File readTaskState = new File(String.format("%s/%s/read_task_labels.csv", experimentDir, shortName));
        writeTaskLabelState(nodeState, readTaskState, readTaskLabels, taskNames);

        File writeTaskState = new File(String.format("%s/%s/write_task_labels.csv", experimentDir, shortName));
        writeTaskLabelState(nodeState, writeTaskState, writeTaskLabels, taskNames);
    }

    private void writeTaskLabelState(File nodeState,
                                     File writeTaskState,
                                     Map<String, Integer> writeTaskLabels,
                                     List<String> taskNames) {
        boolean newlyCreated;
        try {
            newlyCreated = writeTaskState.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(writeTaskState, true))) {
            if (newlyCreated) {
                for (String task : taskNames) {
                    writer.printf("%s,", task);
                }
                writer.println();
            }
            for (String task : taskNames) {
                Integer label = writeTaskLabels.get(task);
                if (label == null) {
                    writer.printf("nan,");
                } else {
                    writer.printf("%d,", label);
                }
            }
            writer.println();
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", nodeState);
            System.exit(1);
        }
    }
}
