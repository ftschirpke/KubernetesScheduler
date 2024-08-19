package labelling;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import labelling.approaches.Approach;
import labelling.approaches.OnlineTaremaApproach;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class TaskSpecificExperiment<T extends Number & Comparable<T>> {
    static String defaultPath = "../Lotaru-traces/traces";
    static boolean includeLocal = true;

    String experimentName;
    String experimentLabel;
    LotaruTraces lotaruTraces = new LotaruTraces();
    List<OnlineTaremaApproach<T>> approaches = new ArrayList<>();
    String experimentDir;
    String task;
    final boolean writeState;

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: TaskSpecificExperiment /path/to/Lotaru-traces/traces <experiment> <label>");
            log.info("Defaulting to path: {}, experiment: {}, label: {}",
                    defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]);
            args = new String[]{defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]};
        }
        if (!ArrayUtils.contains(LotaruTraces.experiments, args[1])) {
            log.error("Experiment {} not found", args[1]);
            System.exit(1);
        }
        if (!ArrayUtils.contains(LotaruTraces.labels, args[2])) {
            log.error("Label {} not found", args[2]);
            System.exit(1);
        }

        double singlePointClusterScore = 0.8;

        TraceField<Long> target = LongField.REALTIME;
        boolean higherIsBetter = false;

        Function<String, Float> nodeWeight = LotaruTraces.nodeCpus::get; // CPU weight
        // Function<String, Float> nodeWeight = LotaruTraces.nodeGigabyteMemory::get; // Memory weight
        // Function<String, Float> nodeWeight = s -> 1.0f; // Equal weight

        boolean writeState = true;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String timeString = formatter.format(System.currentTimeMillis());

        for (String experimentName : LotaruTraces.experiments) {
            for (String label : LotaruTraces.labels) {
                String lotaruTracesDir = args[0];
                LotaruTraces traces = new LotaruTraces();
                File tracesDirectory = new File(lotaruTracesDir);
                try {
                    traces.readTraces(tracesDirectory, experimentName, label);
                } catch (Exception e) {
                    log.info("Error occured: {}", e.getMessage());
                    log.error("File {} does not exist", lotaruTracesDir);
                    System.exit(1);
                }
                for (String task : traces.taskNames) {
                    experiment(traces, experimentName, label, target, nodeWeight, higherIsBetter,
                            singlePointClusterScore, timeString, writeState, task);
                }
            }
        }
    }

    public static <S extends Number & Comparable<S>> void experiment(LotaruTraces lotaruTraces,
                                  String experimentName,
                                  String label,
                                  TraceField<S> target,
                                  Function<String, Float> nodeWeight,
                                  boolean higherIsBetter,
                                  double singlePointClusterScore,
                                  String dirString,
                                  boolean writeState,
                                  String task
    ) {
        TaskSpecificExperiment<S> experiment = new TaskSpecificExperiment<>(experimentName, label, dirString, writeState, task);
        experiment.lotaruTraces = lotaruTraces;

        /*
        Map<String, Map<String, Integer>> counts = new HashMap<>();
        experiment.lotaruTraces.allLinesByTask(true).forEach(
                line -> {
                    String task = experiment.lotaruTraces.getFromLine("Task", line);
                    String node = experiment.lotaruTraces.getFromLine("Machine", line);
                    if (!counts.containsKey(task)) {
                        counts.put(task, new HashMap<>());
                    }
                    counts.get(task).merge(node, 1, Integer::sum);
                }
        );
        int totalMax = 0;
        int totalMin = 40;
        log.info("{} {}", experimentName, label);
        for (String task : counts.keySet()) {
            // log.info("{} ({})", counts.get(task), task);
            int min = counts.get(task).values().stream().min(Integer::compareTo).orElse(0);
            totalMin = Math.min(totalMin, min);
            int max = counts.get(task).values().stream().max(Integer::compareTo).orElse(0);
            totalMax = Math.max(totalMax, max);
        }
        log.info("Total min: {}, Total max: {}", totalMin, totalMax);
        log.info("Abstract task count: {}", counts.size());
        */
        experiment.setApproaches(target, nodeWeight, higherIsBetter, singlePointClusterScore);
        experiment.run();
    }

    TaskSpecificExperiment(String experimentName, String experimentLabel, String dirString, boolean writeState, String task) {
        this.experimentName = experimentName;
        this.experimentLabel = experimentLabel;
        this.experimentDir = String.format("../task-specific-experiments/%s/%s/%s/%s", dirString, experimentName, experimentLabel, task);
        this.writeState = writeState;
        this.task = task;
    }

    void setApproaches(TraceField<T> target,
                                                          Function<String, Float> nodeWeight,
                                                          boolean higherIsBetter,
                                                          double singlePointClusterScore) {

        Stream<String> nodeStream;
        if (includeLocal) {
            nodeStream = Arrays.stream(LotaruTraces.getNodesIncludingLocal());
        } else {
            nodeStream = LotaruTraces.getNodesWithoutLocal().stream();
        }
        Set<String> nodes = nodeStream.collect(Collectors.toSet());

        // approaches.add(new BenchmarkTaremaApproach(singlePointClusterScore));
        // approaches.add(
        //         OnlineTaremaApproach.naive(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes)
        // );
        // approaches.add(
        //         OnlineTaremaApproach.transitive(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes)
        // );
        approaches.add(
                // the task-specific estimator that never uses task-specific labels
                OnlineTaremaApproach.java(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes, 10000L)
        );
        approaches.add(
                // the task-specific estimator that always uses task-specific labels
                OnlineTaremaApproach.java(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes, 0L)
        );
        // approaches.add(
        //         OnlineTaremaApproach.tarema(target, nodeWeight, LotaruTraces.cpuBenchmarks, true, singlePointClusterScore)
        // );
    }

    void applyLine(String[] line, boolean write) {
        String machineName = lotaruTraces.getFromLine("Machine", line);
        String nodeName = LotaruTraces.machineNames.get(machineName);
        TaskConfig config = lotaruTraces.taskConfigFromLine(line);
        TraceRecord trace = lotaruTraces.taskTraceFromLine(line);
        if (write) {
            writeDataLine(line);
        }
        for (OnlineTaremaApproach<T> approach : approaches) {
            approach.onTaskTermination(trace, config, nodeName);
            approach.recalculate();
            if (write) {
                boolean taskSpecific = approach.getName().contains("<0>");
                // HACK: this is very dirty code to check for the task-specific approach

                List<String> onlyTask = List.of(task);
                approach.writeState(experimentDir, lotaruTraces.taskNames, onlyTask);
            }
        }

    }

    void run() {
        List<String[]> lines = lotaruTraces.allLinesFairly(includeLocal).toList();
        if (writeState) {
            File experimentDirectory = new File(experimentDir);
            if (!experimentDirectory.exists()) {
                boolean success = experimentDirectory.mkdirs();
                if (!success) {
                    log.error("Could not create directory {}", experimentDirectory);
                    System.exit(1);
                }
            }
        }
        log.info("EXPERIMENT: {}, LABEL: {}, TASK: {}", experimentName, experimentLabel, task);
        // add all values != task name
        lines.stream().filter(line -> {
            String taskName = lotaruTraces.getFromLine("Task", line);
            return !taskName.equals(task);
        }).forEachOrdered(line -> {
            applyLine(line, false);
        });
        lines.stream().filter(line -> {
            String taskName = lotaruTraces.getFromLine("Task", line);
            return taskName.equals(task);
        }).forEachOrdered(line -> {
            applyLine(line, writeState);
        });
        // log.info("Finished running approaches.");
        // for (Approach approach : approaches) {
        //     System.out.printf("----- Approach %s -----\n", approach.getName());
        //     approach.printNodeLabels();
        // }
        // for (Approach approach : approaches) {
        //     System.out.printf("----- Approach %s -----\n", approach.getName());
        //     approach.printTaskLabels();
        // }
    }

    void writeDataLine(final String[] line) {
        File dir = new File(experimentDir);
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                log.error("Could not create directory {}", dir);
                System.exit(1);
            }
        }

        boolean newlyCreated;
        File rawDataFile = new File(String.format("%s/raw_data.csv", experimentDir));
        try {
            newlyCreated = rawDataFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(rawDataFile, true))) {
            if (newlyCreated) {
                String header = String.join(",", lotaruTraces.csvHeader);
                writer.println(header);
            }
            writer.println(String.join(",", line));
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", rawDataFile);
            System.exit(1);
        }

    }
}