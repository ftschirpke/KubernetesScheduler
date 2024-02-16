package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.approaches.Approach;
import labelling.approaches.TaremaApproach;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class LabelConvergenceExperiment {
    static String defaultPath = "/home/friedrich/bachelor/Lotaru-traces/traces";

    LotaruTraces lotaruTraces = new LotaruTraces();
    NextflowTraceStorage historicTraces = new NextflowTraceStorage(); // traces used for labelling
    TaskLabeller taskLabeller = new TaskLabeller();
    NodeLabeller nodeLabeller = new NodeLabeller();
    List<Approach> approaches = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length != 3) {
            log.error("Usage: LabelConvergenceExperiment /path/to/Lotaru-traces/traces <experiment> <label>");
            log.info("Defaulting to path: {}, experiment: {}, label: {}",
                    defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]);
            args = new String[]{defaultPath, LotaruTraces.experiments[0], LotaruTraces.labels[0]};
            // System.exit(1); // TODO: maybe remove the default and reenable this
        }
        if (!Arrays.asList(LotaruTraces.experiments).contains(args[1])) {
            log.error("Experiment {} not found", args[1]);
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
    }

    void run() {
        for (Approach approach : approaches) {
            approach.initialize();
        }
        int i = 0;
        while (i < 10) { // TODO: replace with a proper condition
            for (Approach approach : approaches) {
                NodeWithAlloc node = LotaruTraces.nodes[0]; // HACK: properly select a node
                String[] line = lotaruTraces.csvData.get(node).get(0); // HACK: properly select a line
                TaskConfig config = lotaruTraces.taskConfigFromLine(line);
                NextflowTraceRecord trace = lotaruTraces.taskTraceFromLine(line);
                approach.onTaskTermination(trace, config, node);
            }
            i++;
        }
    }


    void recalculateTaskLabels() {
        NodeLabeller.GroupWeights groupWeights = nodeLabeller.getGroupWeights();
        if (groupWeights == null) {
            log.info("Online Tarema Scheduler: No group weights available to recalculate task labels.");
            return;
        }
        taskLabeller.recalculateLabels(historicTraces, groupWeights);
        log.info("Online Tarema Scheduler: New task labels are:\n{}", taskLabeller.getLabels());
    }

    public void recalculateNodeLabels(Stream<NodeWithAlloc> nodesWithNewData) {
        boolean changed = nodeLabeller.recalculateLabels(historicTraces, nodesWithNewData);
        if (!changed) {
            return;
        }
        log.info("Online Tarema Scheduler: New node labels are:\n{}", nodeLabeller.getLabels());
    }
}
