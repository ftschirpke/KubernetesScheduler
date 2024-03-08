package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import labelling.approaches.Approach;
import labelling.approaches.BenchmarkTaremaApproach;
import labelling.approaches.OnlineTaremaApproach;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class LabelConvergenceExperiment {
    static String defaultPath = "../Lotaru-traces/traces";
    String experimentName;
    String experimentLabel;
    LotaruTraces lotaruTraces = new LotaruTraces();
    List<Approach> approaches = new ArrayList<>();
    String experimentDir;

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

        double[] scores = {0.5, 0.66, 0.8, 0.9};
        TraceField<Long> target = LongField.REALTIME;
        boolean higherIsBetter = false;

        for (double singlePointClusterScore : scores) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
            String timeString = formatter.format(System.currentTimeMillis());
            timeString = String.format("%d-%s", (int) (singlePointClusterScore * 100.0), timeString);

            for (String experimentName : LotaruTraces.experiments) {
                for (String label : LotaruTraces.labels) {
                    experiment(args[0], experimentName, label, target, higherIsBetter, singlePointClusterScore,
                            timeString);
                }
            }
        }
    }

    public static <T extends Number & Comparable<T>> void experiment(String lotaruTracesDir,
                                                                     String experimentName,
                                                                     String label,
                                                                     TraceField<T> target,
                                                                     boolean higherIsBetter,
                                                                     double singlePointClusterScore,
                                                                     String timeString) {
        LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(experimentName, label, timeString);
        experiment.initializeTraces(lotaruTracesDir);
        experiment.setApproaches(target, higherIsBetter, singlePointClusterScore);
        experiment.run();
    }

    LabelConvergenceExperiment(String experimentName, String experimentLabel, String timeString) {
        this.experimentName = experimentName;
        this.experimentLabel = experimentLabel;
        this.experimentDir = String.format("../label-experiments/results/%s/%s/%s", timeString, experimentName, experimentLabel);
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

    <T extends Number & Comparable<T>> void setApproaches(TraceField<T> target, boolean higherIsBetter, double singlePointClusterScore) {
        approaches.add(new BenchmarkTaremaApproach(singlePointClusterScore));
        approaches.add(OnlineTaremaApproach.naive(target, higherIsBetter, singlePointClusterScore));
        approaches.add(OnlineTaremaApproach.transitive(target, higherIsBetter, singlePointClusterScore));
        approaches.add(OnlineTaremaApproach.tarema(target, LotaruTraces.cpuBenchmarks, true, singlePointClusterScore));
    }

    void run() {
        Stream<String[]> lines = lotaruTraces.allLinesByTask();
        File experimentDirectory = new File(experimentDir);
        if (!experimentDirectory.exists()) {
            boolean success = experimentDirectory.mkdirs();
            if (!success) {
                log.error("Could not create directory {}", experimentDirectory);
                System.exit(1);
            }
        }
        lines.forEachOrdered(line -> {
            String machineName = lotaruTraces.getFromLine("Machine", line);
            NodeWithAlloc node = LotaruTraces.machineNames.get(machineName);
            TaskConfig config = lotaruTraces.taskConfigFromLine(line);
            TraceRecord trace = lotaruTraces.taskTraceFromLine(line);
            for (Approach approach : approaches) {
                approach.onTaskTermination(trace, config, node);
                approach.recalculate();
                approach.writeState(experimentDir, lotaruTraces.taskNames);
            }
        });
        log.info("Finished running approaches.");
        for (Approach approach : approaches) {
            System.out.printf("----- Approach %s -----\n", approach.getName());
            approach.printNodeLabels();
        }
        for (Approach approach : approaches) {
            System.out.printf("----- Approach %s -----\n", approach.getName());
            approach.printTaskLabels();
        }
    }
}
