package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class NodeFirstLabeller {
    @Getter
    private Labels maxLabels = null;
    @Getter
    private final Map<NodeWithAlloc, Labels> labels = new HashMap<>();
    private final Map<NodeWithAlloc, NodeSpeedEstimation> estimations = new HashMap<>();
    private final String modelDirectory; // directory for the python scripts to store the models

    public NodeFirstLabeller() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        this.modelDirectory = "external/node-first-labeller/" + formatter.format(System.currentTimeMillis());
    }

    public record NodeLabelState(Labels maxLabels, Map<NodeWithAlloc, Labels> labels, GroupWeights groupWeights) {
    }

    public static NodeLabelState labelOnce(Map<NodeWithAlloc, NodeSpeedEstimation> estimations) {
        NodeFirstLabeller labeller = new NodeFirstLabeller();
        labeller.estimations.putAll(estimations);
        labeller.kmeansClustering();
        Labels maxLabels = labeller.getMaxLabels();
        Map<NodeWithAlloc, Labels> labels = labeller.getLabels();
        GroupWeights groupWeights = GroupWeights.forNodeLabels(maxLabels, labels);
        return new NodeLabelState(maxLabels, labels, groupWeights);
    }

    private void runBayesForNode(NextflowTraceStorage traces, NodeWithAlloc node, List<Integer> ids) {
        log.info("Online Tarema Scheduler: Running bayes.py for node {}", node.getName());
        Process bayesProcess;
        int exitCode;
        synchronized (modelDirectory) { // ensure that only one process is writing to the same model directory
            try {
                String nodeModelDirectory = modelDirectory + "/" + node.getName();
                bayesProcess = new ProcessBuilder(
                        "external/venv/bin/python3", "external/node-first-bayes.py", nodeModelDirectory).start();
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to start bayes.py process", e);
                return;
            }
            log.info("DEBUG 4");
            OutputStream in = bayesProcess.getOutputStream();
            PrintWriter writer = new PrintWriter(in);

            writeValues(writer, "cpus", traces.getForIds(ids.stream(), FloatField.CPUS));
            writeValues(writer, "%cpu", traces.getForIds(ids.stream(), FloatField.CPU_PERCENTAGE));
            writeValues(writer, "vmem", traces.getForIds(ids.stream(), LongField.VIRTUAL_MEMORY));
            writeValues(writer, "rss", traces.getForIds(ids.stream(), LongField.RESIDENT_SET_SIZE));
            writeValues(writer, "rchar", traces.getForIds(ids.stream(), LongField.CHARACTERS_READ));
            writeValues(writer, "wchar", traces.getForIds(ids.stream(), LongField.CHARACTERS_WRITTEN));
            writeValues(writer, "realtime", traces.getForIds(ids.stream(), LongField.REALTIME));

            writer.flush();
            writer.close();
            log.info("DEBUG 5");
            try {
                boolean finished = bayesProcess.waitFor(5, TimeUnit.SECONDS);
                if (!finished) {
                    log.error("Online Tarema Scheduler: bayes.py timed out");
                    bayesProcess.destroy();
                    return;
                }
                exitCode = bayesProcess.exitValue();
                log.info("DEBUG 6");
                log.info("Online Tarema Scheduler: bayes.py exited with code {}", exitCode);
            } catch (InterruptedException e) {
                log.error("Online Tarema Scheduler: Failed to wait for bayes.py to exit", e);
                return;
            }
        }
        log.info("DEBUG 7");

        String line;
        if (exitCode != 0) {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            log.info("DEBUG 8");
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read bayes.py stderr", e);
            }
            return;
        }
        log.info("DEBUG 9");

        MeanStdRecord cpu = null;
        MeanStdRecord mem = null;
        MeanStdRecord read = null;
        MeanStdRecord write = null;
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
        log.info("DEBUG 10");
        try {
            log.info("DEBUG 11");
            while ((line = stdoutReader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
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
                    log.error("Online Tarema Scheduler: bayes.py UNEXPECTED: {}", line);
                }
            }
            log.info("DEBUG 12");
            if (cpu == null || mem == null || read == null || write == null) {
                log.error("Online Tarema Scheduler: could not find all estimations in bayes.py output");
                return;
            }
            log.info("DEBUG 13");
            estimations.put(node, new NodeSpeedEstimation(cpu, mem, read, write));
            log.info("DEBUG 14");
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read bayes.py stdout", e);
        }
        log.info("DEBUG 15");
    }

    public boolean recalculateLabels(NextflowTraceStorage traces, Map<NodeWithAlloc, List<Integer>> nodesWithNewTraces) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return false;
        }
        for (Map.Entry<NodeWithAlloc, List<Integer>> entry : nodesWithNewTraces.entrySet()) {
            NodeWithAlloc node = entry.getKey();
            List<Integer> ids = entry.getValue();
            runBayesForNode(traces, node, ids);
        }
        return kmeansClustering();
    }

    private boolean kmeansClustering() {
        log.info("Online Tarema Scheduler: Running kmeans.py");
        Process kmeansProcess;
        try {
            kmeansProcess = new ProcessBuilder("external/venv/bin/python3", "external/kmeans.py").start();
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to start kmeans.py process", e);
            return false;
        }
        OutputStream in = kmeansProcess.getOutputStream();
        PrintWriter writer = new PrintWriter(in);

        Map<String, NodeWithAlloc> nodesWritten = new HashMap<>();
        for (Map.Entry<NodeWithAlloc, NodeSpeedEstimation> entry : estimations.entrySet()) {
            NodeWithAlloc node = entry.getKey();
            String nodeName = node.getName();
            NodeSpeedEstimation estimation = entry.getValue();
            writeValues(writer, nodeName, estimation.means());
            nodesWritten.put(nodeName, node);
        }

        writer.flush();
        writer.close();
        int exitCode;
        try {
            exitCode = kmeansProcess.waitFor();
            log.info("Online Tarema Scheduler: kmeans.py exited with code {}", exitCode);
        } catch (InterruptedException e) {
            log.error("Online Tarema Scheduler: Failed to wait for kmeans.py to exit", e);
            return false;
        }

        String line;
        if (exitCode != 0) {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(kmeansProcess.getErrorStream()));
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: kmeans.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read kmeans.py stderr", e);
            }
            return false;
        }

        boolean labelsChanged = false;
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(kmeansProcess.getInputStream()));
        try {
            Labels newMaxLabels = null;
            Map<NodeWithAlloc, Labels> newNodeLabels = new HashMap<>();
            while ((line = stdoutReader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("DEBUG")) {
                    log.info("Online Tarema Scheduler: kmeans.py {}", line);
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length != 5) {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED: {}", line);
                    continue;
                }
                String rowName = parts[0];
                int cpu, mem, read, write;
                try {
                    cpu = Integer.parseInt(parts[1]);
                    mem = Integer.parseInt(parts[2]);
                    read = Integer.parseInt(parts[3]);
                    write = Integer.parseInt(parts[4]);
                    log.info("Online Tarema Scheduler: kmeans.py: {} {} {} {} {}", rowName, cpu, mem, read, write);
                } catch (NumberFormatException e) {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED: {}", line);
                    continue;
                }

                Labels newLabels = new Labels(cpu, mem, read, write);
                if (rowName.equals("maxLabels")) {
                    if (!newLabels.equals(maxLabels)) {
                        newMaxLabels = newLabels;
                    }
                } else if (nodesWritten.containsKey(rowName)) {
                    NodeWithAlloc node = nodesWritten.get(rowName);
                    Labels nodeLabels = labels.get(node);
                    if (!newLabels.equals(nodeLabels)) {
                        newNodeLabels.put(node, newLabels);
                    }
                } else {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED {}", rowName);
                }
            }
            if (newMaxLabels != null) {
                maxLabels = newMaxLabels;
                labelsChanged = true;
            }
            if (!newNodeLabels.isEmpty()) {
                labels.putAll(newNodeLabels);
                labelsChanged = true;
            }
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read kmeans.py stdout", e);
        }
        return labelsChanged;
    }

    private <T> void writeValues(PrintWriter writer, String name, Stream<T> values) {
        writer.write(name);
        values.forEachOrdered(val -> writer.write("," + val));
        writer.write("\n");
    }

    public record NodeSpeedEstimation(MeanStdRecord cpu, MeanStdRecord mem, MeanStdRecord read, MeanStdRecord write) {
        public NodeSpeedEstimation(float cpu, float mem, float read, float write) {
            this(new MeanStdRecord(cpu, 0), new MeanStdRecord(mem, 0), new MeanStdRecord(read, 0), new MeanStdRecord(write, 0));
        }

        public Stream<Float> means() {
            return Stream.of(cpu.mean(), mem.mean(), read.mean(), write.mean());
        }
    }

    private record MeanStdRecord(float mean, float std) {
    }

    private MeanStdRecord meanStdFromLine(String line) throws NumberFormatException {
        String[] parts = line.split(",");
        try {
            return new MeanStdRecord(-Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
            // HACK: negative mean to inverse the order (higher mean (slower node) -> lower label)
        } catch (NumberFormatException e) {
            log.error("Online Tarema Scheduler: Failed to parse mean and std from line \"{}\"", line);
            return null;
        }
    }
}
