package labelling;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import labelling.approaches.Approach;
import labelling.approaches.BenchmarkTaremaApproach;
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
public class LabelConvergenceExperiment {
    static String defaultPath = "../Lotaru-traces/traces";
    static boolean includeLocal = true;

    String experimentName;
    String experimentLabel;
    LotaruTraces lotaruTraces = new LotaruTraces();
    List<Approach> approaches = new ArrayList<>();
    String experimentDir;
    final boolean writeState;

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: LabelConvergenceExperiment /path/to/Lotaru-traces/traces <experiment> <label>");
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

        double[] scores = {0, 0.5, 0.66, 0.8, 0.9};
        TraceField<Long> target = LongField.CHARACTERS_READ;
        // Function<String, Float> nodeWeight = LotaruTraces.nodeGigabyteMemory::get;
        Function<String, Float> nodeWeight = s -> 1.0f;
        boolean higherIsBetter = false;

        boolean writeState = true;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String timeString = formatter.format(System.currentTimeMillis());

        for (double singlePointClusterScore : scores) {
            String localStr = includeLocal ? "local" : "nolocal";
            String fullString = String.format("%s-rchar/%s-%d", timeString, localStr, (int) (singlePointClusterScore * 100.0));

            for (String experimentName : LotaruTraces.experiments) {
                for (String label : LotaruTraces.labels) {
                    experiment(args[0], experimentName, label, target, nodeWeight, higherIsBetter,
                            singlePointClusterScore, fullString, writeState);
                }
            }
        }
    }

    public static <T extends Number & Comparable<T>> void experiment(String lotaruTracesDir,
                                                                     String experimentName,
                                                                     String label,
                                                                     TraceField<T> target,
                                                                     Function<String, Float> nodeWeight,
                                                                     boolean higherIsBetter,
                                                                     double singlePointClusterScore,
                                                                     String dirString,
                                                                     boolean writeState) {
        LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(experimentName, label, dirString, writeState);
        experiment.initializeTraces(lotaruTracesDir);
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
        log.info("{} {}", experimentName, label);
        for (String task : counts.keySet()) {
            log.info("{} ({})", counts.get(task), task);
        }
       // experiment.setApproaches(target, nodeWeight, higherIsBetter, singlePointClusterScore);
       // experiment.run();
    }

    LabelConvergenceExperiment(String experimentName, String experimentLabel, String dirString, boolean writeState) {
        this.experimentName = experimentName;
        this.experimentLabel = experimentLabel;
        this.experimentDir = String.format("../label-experiments/results/%s/%s/%s", dirString, experimentName, experimentLabel);
        this.writeState = writeState;
    }

    void initializeTraces(String lotaruTracesDir) {
        File tracesDirectory = new File(lotaruTracesDir);
        try {
            lotaruTraces.readTraces(tracesDirectory, experimentName, experimentLabel);
        } catch (Exception e) {
            log.info("Error occured: {}", e.getMessage());
            log.error("File {} does not exist", lotaruTracesDir);
            System.exit(1);
        }
    }

    <T extends Number & Comparable<T>> void setApproaches(TraceField<T> target,
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

        approaches.add(new BenchmarkTaremaApproach(singlePointClusterScore));
        approaches.add(
                OnlineTaremaApproach.naive(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes)
        );
        approaches.add(
                OnlineTaremaApproach.transitive(target, nodeWeight, higherIsBetter, singlePointClusterScore, nodes)
        );
        approaches.add(
                OnlineTaremaApproach.tarema(target, nodeWeight, LotaruTraces.cpuBenchmarks, true, singlePointClusterScore)
        );
    }

    void run() {
        Stream<String[]> lines = lotaruTraces.allLinesFairly(includeLocal);
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
        lines.forEachOrdered(line -> {
            String machineName = lotaruTraces.getFromLine("Machine", line);
            String nodeName = LotaruTraces.machineNames.get(machineName);
            TaskConfig config = lotaruTraces.taskConfigFromLine(line);
            TraceRecord trace = lotaruTraces.taskTraceFromLine(line);
            if (writeState) {
                writeDataLine(line);
            }
            for (Approach approach : approaches) {
                approach.onTaskTermination(trace, config, nodeName);
                approach.recalculate();
                if (writeState) {
                    approach.writeState(experimentDir, lotaruTraces.taskNames);
                }
            }
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
