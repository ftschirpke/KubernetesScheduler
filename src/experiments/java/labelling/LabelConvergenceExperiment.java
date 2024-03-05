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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class LabelConvergenceExperiment {
    static String defaultPath = "/home/friedrich/bachelor/Lotaru-traces/traces";
    String experimentName;
    String experimentLabel;
    LotaruTraces lotaruTraces = new LotaruTraces();
    List<Approach> approaches = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: LabelConvergenceExperiment /path/to/Lotaru-traces/traces <experiment> <label>");
            log.info("Defaulting to path: {}, experiment: {}, label: {}",
                    defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]);
            args = new String[]{defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]};
            // System.exit(1); // TODO: maybe remove the default and reenable this
        }
        if (!ArrayUtils.contains(LotaruTraces.experiments, args[1])) {
            log.error("Experiment {} not found", args[1]);
            System.exit(1);
        }
        if (!ArrayUtils.contains(LotaruTraces.labels, args[2])) {
            log.error("Label {} not found", args[2]);
            System.exit(1);
        }

        boolean all = false;
        double onePointClusterScore = 0.0;

        if (all) {
            for (String experimentName : LotaruTraces.experiments) {
                for (String label : LotaruTraces.labels) {
                    experiment(args[0], experimentName, label, LongField.REALTIME, onePointClusterScore);
                }
            }
        } else {
            experiment(args[0], args[1], args[2], LongField.REALTIME, onePointClusterScore);
        }
    }

    public static <T extends Number & Comparable<T>> void experiment(String lotaruTracesDir,
                                      String experimentName,
                                      String label,
                                      TraceField<T> target,
                                      double onePointClusterScore) {
        LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(experimentName, label);
        experiment.initializeTraces(lotaruTracesDir);
        experiment.setApproaches(target, onePointClusterScore);
        experiment.run();
    }

    LabelConvergenceExperiment(String experimentName, String experimentLabel) {
        this.experimentName = experimentName;
        this.experimentLabel = experimentLabel;
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
        System.out.println("Finished reading traces.");
    }

    <T extends Number & Comparable<T>> void setApproaches(TraceField<T> target, double onePointClusterScore) {
        approaches.add(new BenchmarkTaremaApproach(onePointClusterScore));
        approaches.add(OnlineTaremaApproach.naive(target, onePointClusterScore));
        approaches.add(OnlineTaremaApproach.smart(target, onePointClusterScore));
        System.out.println("Finished adding approaches.");
    }

    void run() {
        log.info("Finished initializing approaches.");
        Stream<String[]> lines = lotaruTraces.allLinesByTask();
        lines.forEachOrdered(line -> {
            String machineName = lotaruTraces.getFromLine("Machine", line);
            NodeWithAlloc node = LotaruTraces.machineNames.get(machineName);
            TaskConfig config = lotaruTraces.taskConfigFromLine(line);
            TraceRecord trace = lotaruTraces.taskTraceFromLine(line);
            for (Approach approach : approaches) {
                approach.onTaskTermination(trace, config, node);
            }
        });
        log.info("Finished running approaches.");

        // TODO: move this into the body above if we want to analyze the convergence
        for (Approach approach : approaches) {
            approach.recalculate();
        }
        for (Approach approach : approaches) {
            log.info("Approach {} has node labels:", approach.getName());
            approach.printNodeLabels();
        }
        for (Approach approach : approaches) {
            System.out.printf("-- Approach %s --\n", approach.getName());
            approach.printNodeLabels();
        }
    }
}
