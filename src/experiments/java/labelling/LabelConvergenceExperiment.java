package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import labelling.approaches.Approach;
import labelling.approaches.NaiveOnlineTaremaApproach;
import labelling.approaches.TaremaApproach;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

        if (all) {
            for (String experimentName : LotaruTraces.experiments) {
                for (String label : LotaruTraces.labels) {
                    LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(time, experimentName, label);
                    experiment.initializeTraces(args[0]);
                    experiment.setApproaches();
                    experiment.run();
                }
            }
        } else {
            LabelConvergenceExperiment experiment = new LabelConvergenceExperiment(time, args[1], args[2]);
            experiment.initializeTraces(args[0]);
            experiment.setApproaches();
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

    void setApproaches() {
        approaches.add(new TaremaApproach());
        approaches.add(new NaiveOnlineTaremaApproach());
        System.out.println("Finished adding approaches.");
    }

    void run() {
        for (Approach approach : approaches) {
            approach.initialize();
        }
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
            log.info("Approach {} has node labels:", approach.getClass().getName());
            for (NodeWithAlloc showNode : LotaruTraces.nodes) {
                log.info("Node \"{}\": {}", showNode.getName(), approach.getNodeLabels().get(showNode));
            }
        }
        // write labels to file
        try {
            String directory = "/home/friedrich/bachelor/scheduler/results/" + experimentTimeStamp;
            if (new File(directory).mkdir()) {
                log.info("Directory {} created", directory);
            }
            String name = experimentName + "-" + experimentLabel;
            File file = new File(directory + "/" + name + ".txt");
            FileWriter writer = new FileWriter(file);
            for (Approach approach : approaches) {
                writer.write(String.format("-- Approach %s --\n", approach.getClass().getName()));
                for (NodeWithAlloc showNode : LotaruTraces.nodes) {
                    writer.write(String.format("Node \"%s\": %s\n", showNode.getName(), approach.getNodeLabels().get(showNode)));
                }
            }
            writer.close();
        } catch (IOException e) {
            log.error("Error writing to file: {}", e.getMessage());
        }
    }
}
