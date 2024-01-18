package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Getter
@Slf4j
public class NodeLabeller {
    private final int labelSpaceSize;
    private final HashMap<String, Labels> labels;

    public NodeLabeller(int labelSpaceSize) {
        this.labelSpaceSize = labelSpaceSize;
        // HACK: hard-coded labels for now
        this.labels = new HashMap<>(Map.of(
                "hu-worker-c29", new Labels(labelSpaceSize, 1, 1, 1, 1),
                "hu-worker-c40", new Labels(labelSpaceSize, 2, 2, 2, 2),
                "hu-worker-c43", new Labels(labelSpaceSize, 3, 3, 3, 3)
        ));
    }

    private void runBayesForNode(NextflowTraceStorage traces, String nodeName) {
        log.info("Online Tarema Scheduler: Running bayes.py for node {}", nodeName);
        Process bayesProcess;
        try {
            bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/bayes.py").start();
        } catch (IOException e) {
            log.error("Failed to start bayes.py process", e);
            return;
        }
        OutputStream in = bayesProcess.getOutputStream();
        PrintWriter writer = new PrintWriter(in);

        Stream<Float> cpus = traces.getForNode(nodeName, NextflowTraceStorage.FloatField.CPUS);
        cpus.forEachOrdered(f -> writer.write(f + ","));
        writer.write("cpus\n");

        Stream<Float> cpuPercentages = traces.getForNode(nodeName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
        cpuPercentages.forEachOrdered(f -> writer.write(f + ","));
        writer.write("cpu_percentage\n");

        Stream<Long> runtimes = traces.getForNode(nodeName, NextflowTraceStorage.LongField.REALTIME);
        runtimes.forEachOrdered(f -> writer.write(f + ","));
        writer.write("runtime\n");

        writer.flush();
        writer.close();
        int exitCode;
        try {
            exitCode = bayesProcess.waitFor();
            log.info("Online Tarema Scheduler: bayes.py exited with code {}", exitCode);
        } catch (InterruptedException e) {
            log.error("Failed to wait for bayes.py to exit", e);
            return;
        }

        String line;
        if (exitCode == 0) {
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
            try {
                while ((line = stdoutReader.readLine()) != null) {
                    log.info("Online Tarema Scheduler: bayes.py stdout: {}", line);
                }
            } catch (IOException e) {
                log.error("Failed to read bayes.py stdout", e);
            }
        } else {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Failed to read bayes.py stderr", e);
            }
        }

    }

    public void recalculateLabels(NextflowTraceStorage traces) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return;
        }
        log.info("Not calculating labels for now, just testing the interaction with the bayes.py script");
        traces.getNodeNames().forEach(nodeName -> runBayesForNode(traces, nodeName));
    }
}
