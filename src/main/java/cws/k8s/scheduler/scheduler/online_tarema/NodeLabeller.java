package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Getter
@Slf4j
public class NodeLabeller {
    private final int labelSpaceSize;
    private final HashMap<String, Labels> labels;
    private final HashMap<String, Estimation> estimations;

    public NodeLabeller(int labelSpaceSize) {
        this.labelSpaceSize = labelSpaceSize;
        // HACK: hard-coded labels for now
        this.labels = new HashMap<>();
        this.estimations = new HashMap<>();
    }

    private void runBayesForNode(NextflowTraceStorage traces, String nodeName) {
        log.info("Online Tarema Scheduler: Running bayes.py for node {}", nodeName);
        Process bayesProcess;
        try {
            bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/bayes.py").start();
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to start bayes.py process", e);
            return;
        }
        OutputStream in = bayesProcess.getOutputStream();
        PrintWriter writer = new PrintWriter(in);

        writeValues(writer, "cpus", traces.getForNode(nodeName, NextflowTraceStorage.FloatField.CPUS));
        writeValues(writer, "%cpu", traces.getForNode(nodeName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE));
        writeValues(writer, "vmem", traces.getForNode(nodeName, NextflowTraceStorage.LongField.VIRTUAL_MEMORY));
        writeValues(writer, "rss", traces.getForNode(nodeName, NextflowTraceStorage.LongField.RESIDENT_SET_SIZE));
        writeValues(writer, "rchar", traces.getForNode(nodeName, NextflowTraceStorage.LongField.CHARACTERS_READ));
        writeValues(writer, "wchar", traces.getForNode(nodeName, NextflowTraceStorage.LongField.CHARACTERS_WRITTEN));
        writeValues(writer, "realtime", traces.getForNode(nodeName, NextflowTraceStorage.LongField.REALTIME));

        writer.flush();
        writer.close();
        int exitCode;
        try {
            exitCode = bayesProcess.waitFor();
            log.info("Online Tarema Scheduler: bayes.py exited with code {}", exitCode);
        } catch (InterruptedException e) {
            log.error("Online Tarema Scheduler: Failed to wait for bayes.py to exit", e);
            return;
        }

        String line;
        MeanStdRecord cpu = null;
        MeanStdRecord mem = null;
        MeanStdRecord read = null;
        MeanStdRecord write = null;
        if (exitCode == 0) {
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
            try {
                while ((line = stdoutReader.readLine()) != null) {
                    if (line.startsWith("DEBUG")) {
                        log.info("Online Tarema Scheduler: bayes.py {}", line);
                    } else if (line.startsWith("CPU")) {
                        cpu = meanStdFromLine(line);
                    } else if (line.startsWith("MEM")) {
                        mem = meanStdFromLine(line);
                    } else if (line.startsWith("SEQ_READ")) {
                        read = meanStdFromLine(line);
                    } else if (line.startsWith("SEQ_WRITE")) {
                        write = meanStdFromLine(line);
                    } else {
                        log.error("Online Tarema Scheduler: bayes.py UNEXPECTED {}", line);
                    }
                }
                if (cpu == null || mem == null || read == null || write == null) {
                    log.error("Online Tarema Scheduler: bayes.py did not return all estimations");
                    return;
                }
                estimations.put(nodeName, new Estimation(cpu, mem, read, write));
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read bayes.py stdout", e);
            }
        } else {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read bayes.py stderr", e);
            }
        }

    }

    public void recalculateLabels(NextflowTraceStorage traces) {
        recalculateLabels(traces, traces.getNodeNames().stream());
    }

    public void recalculateLabels(NextflowTraceStorage traces, Stream<String> nodesWithNewTraces) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return;
        }
        nodesWithNewTraces.forEach(nodeName -> runBayesForNode(traces, nodeName));
        recalculateGroups();
    }

    private void recalculateGroups() {
        // TODO: cluster nodes into groups based on the estimations using silhouette score
    }

    private <T> void writeValues(PrintWriter writer, String name, Stream<T> values) {
        values.forEachOrdered(f -> writer.write(f + ","));
        writer.write(name + "\n");
    }

    private record Estimation(MeanStdRecord cpu, MeanStdRecord mem, MeanStdRecord read, MeanStdRecord write) {
    }

    private record MeanStdRecord(float mean, float std) {
    }

    private MeanStdRecord meanStdFromLine(String line) {
        String[] parts = line.split(",");
        return new MeanStdRecord(Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
    }
}
