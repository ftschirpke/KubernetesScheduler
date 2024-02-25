package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class NodeFirstLabeller {
    @Getter
    private Labels maxLabels = null;
    @Getter
    private final Map<NodeWithAlloc, Labels> labels = new HashMap<>();
    private final Map<NodeWithAlloc, NodeSpeedEstimation> estimations = new HashMap<>();
    private final String modelDirectory; // directory for the python scripts to store the models

    private Process bayesProcess = null;
    private BufferedReader stdoutReader = null;
    private BufferedReader stderrReader = null;
    private PrintWriter stdinWriter = null;

    public NodeFirstLabeller() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        modelDirectory = "external/node-first-labeller/" + formatter.format(System.currentTimeMillis());

    }

    public record NodeLabelState(Labels maxLabels, Map<NodeWithAlloc, Labels> labels, GroupWeights groupWeights) {
    }

    public static NodeLabelState labelOnce(Map<NodeWithAlloc, NodeSpeedEstimation> estimations) throws IOException {
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
        if (bayesProcess == null) {
            try {
                bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/new-node-first-bayes.py").start();
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to start bayes.py process", e);
                return;
            }
            stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
            stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            stdinWriter = new PrintWriter(new OutputStreamWriter(bayesProcess.getOutputStream()), true);
        }
        synchronized (modelDirectory) { // ensure that only one process is writing to the same model directory
            ids.forEach(id -> stdinWriter.println(traces.asString(id)));

            String line;

            MeanStdRecord cpu = null;
            MeanStdRecord mem = null;
            MeanStdRecord read = null;
            MeanStdRecord write = null;
            try {
                int skip = ids.size() - 1;
                while (true) {
                    line = stdoutReader.readLine();
                    if (line == null) {
                        log.error("Online Tarema Scheduler: bayes.py stdout closed unexpectedly");
                        while ((line = stderrReader.readLine()) != null) {
                            log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                        }
                        return;
                    }
                    if (line.startsWith("DEBUG")) {
                        log.info("Online Tarema Scheduler: bayes.py {}", line);
                    } else if (skip > 0) {
                        skip--;
                    } else {
                        break;
                    }
                }
                String[] parts = line.split(";");
                for (String part : parts) {
                    if (part.isEmpty()) {
                        continue;
                    }
                    if (part.startsWith("CPU")) {
                        cpu = meanStdFromString(part);
                    } else if (part.startsWith("MEM")) {
                        mem = meanStdFromString(part);
                    } else if (part.startsWith("SEQ_READ")) {
                        read = meanStdFromString(part);
                    } else if (part.startsWith("SEQ_WRITE")) {
                        write = meanStdFromString(part);
                    } else {
                        log.error("Online Tarema Scheduler: bayes.py UNEXPECTED: {}", part);
                    }
                }
                if (cpu == null || mem == null || read == null || write == null) {
                    log.error("Online Tarema Scheduler: could not find all estimations in bayes.py output");
                    return;
                }
                estimations.put(node, new NodeSpeedEstimation(cpu, mem, read, write));
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read bayes.py stdout", e);
            }
        }
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

    private MeanStdRecord meanStdFromString(String s) throws NumberFormatException {
        String[] parts = s.split(",");
        try {
            return new MeanStdRecord(-Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
            // HACK: negative mean to inverse the order (higher mean (slower node) -> lower label)
        } catch (NumberFormatException e) {
            log.error("Online Tarema Scheduler: Failed to parse mean and std from line \"{}\"", s);
            return null;
        }
    }
}
