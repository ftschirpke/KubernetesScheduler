package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.approaches.Approach;
import labelling.approaches.NaiveOnlineTaremaApproach;
import labelling.approaches.TaremaApproach;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class LabelConvergenceExperiment {
    static String defaultPath = "/home/friedrich/bachelor/Lotaru-traces/traces";

    LotaruTraces lotaruTraces = new LotaruTraces();
    NextflowTraceStorage historicTraces = new NextflowTraceStorage(); // traces used for labelling
    TaskSecondLabeller taskSecondLabeller = new TaskSecondLabeller();
    NodeFirstLabeller nodeFirstLabeller = new NodeFirstLabeller();
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

        LabelConvergenceExperiment experiment = new LabelConvergenceExperiment();
        experiment.initializeTraces(args[0], args[1], args[2]);
        experiment.initializeApproaches();
        experiment.run();
    }

    void initializeTraces(String lotaruTracesDir, String experimentName, String experimentLabel) {
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

    void initializeApproaches() {
        approaches.add(new TaremaApproach());
        approaches.add(new NaiveOnlineTaremaApproach());
    }

    void run() {
        for (Approach approach : approaches) {
            approach.initialize();
        }
        Stream<String[]> lines = lotaruTraces.allLinesByTask();
        lines.forEachOrdered(line -> {
            String machineName = lotaruTraces.getFromLine("Machine", line);
            NodeWithAlloc node = LotaruTraces.machineNames.get(machineName);
            TaskConfig config = lotaruTraces.taskConfigFromLine(line);
            NextflowTraceRecord trace = lotaruTraces.taskTraceFromLine(line);
            log.info("Task trace: {}", trace);
            for (Approach approach : approaches) {
                approach.onTaskTermination(trace, config, node);
            }
            for (Approach approach : approaches) {
                log.info("Approach {} has node labels:", approach.getClass().getName());
                for (NodeWithAlloc showNode : LotaruTraces.nodes) {
                    log.info("Node \"{}\": {}", showNode.getName(), approach.getNodeLabels().get(showNode));
                }
            }
        });
    }
}
