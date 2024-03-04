package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
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
    static String defaultPath = "/home/friedrich/bachelor/Lotaru-traces/traces";
    String experimentTimeStamp;
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

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd--HH-mm-ss");
        String time = formatter.format(System.currentTimeMillis());

        double onePointClusterScore = 0.0;

        if (all) {
            for (String experimentName : LotaruTraces.experiments) {
                for (String label : LotaruTraces.labels) {
                    LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(time, experimentName, label);
                    experiment.initializeTraces(args[0]);
                    experiment.setApproaches(onePointClusterScore);
                    experiment.run();
                }
            }
        } else {
            LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(time, args[1], args[2]);
            experiment.initializeTraces(args[0]);
            experiment.setApproaches(onePointClusterScore);
            experiment.run();
        }
    }

    LabelConvergenceExperiment(String experimentTimeStamp, String experimentName, String experimentLabel) {
        this.experimentTimeStamp = experimentTimeStamp;
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

    void setApproaches(double onePointClusterScore) {
        approaches.add(new BenchmarkTaremaApproach(onePointClusterScore));
        approaches.add(OnlineTaremaApproach.naive(onePointClusterScore));
        approaches.add(OnlineTaremaApproach.smart(onePointClusterScore));
        System.out.println("Finished adding approaches.");
    }

    void run() {
        log.info("Finished initializing approaches.");
        Stream<String[]> lines = lotaruTraces.allLinesByTask();
        lines.forEachOrdered(line -> {
            String machineName = lotaruTraces.getFromLine("Machine", line);
            NodeWithAlloc node = LotaruTraces.machineNames.get(machineName);
            TaskConfig config = lotaruTraces.taskConfigFromLine(line);
            NextflowTraceRecord trace = lotaruTraces.taskTraceFromLine(line);
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
