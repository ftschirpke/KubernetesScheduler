package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class NodeLabeller {
    @Getter
    private Labels maxLabels;
    @Getter
    private final Map<NodeWithAlloc, Labels> labels;
    private final Map<NodeWithAlloc, NodeSpeedEstimation> estimations;

    public NodeLabeller() {
        this.maxLabels = null;
        this.labels = new HashMap<>();
        this.estimations = new HashMap<>();
    }

    public record NodeLabelState(Labels maxLabels, Map<NodeWithAlloc, Labels> labels, GroupWeights groupWeights) {
    }

    public static NodeLabelState labelOnce(Map<NodeWithAlloc, NodeSpeedEstimation> estimations) {
        NodeLabeller labeller = new NodeLabeller();
        labeller.estimations.putAll(estimations);
        labeller.recalculateGroups();
        return new NodeLabelState(labeller.getMaxLabels(), labeller.getLabels(), labeller.getGroupWeights());
    }

    private void runBayesForNode(NextflowTraceStorage traces, NodeWithAlloc node) {
        log.info("Online Tarema Scheduler: Running bayes.py for node {}", node.getName());
        Process bayesProcess;
        try {
            bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/bayes.py").start();
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to start bayes.py process", e);
            return;
        }
        OutputStream in = bayesProcess.getOutputStream();
        PrintWriter writer = new PrintWriter(in);

        writeValues(writer, "cpus", traces.getForNode(node, NextflowTraceStorage.FloatField.CPUS));
        writeValues(writer, "%cpu", traces.getForNode(node, NextflowTraceStorage.FloatField.CPU_PERCENTAGE));
        writeValues(writer, "vmem", traces.getForNode(node, NextflowTraceStorage.LongField.VIRTUAL_MEMORY));
        writeValues(writer, "rss", traces.getForNode(node, NextflowTraceStorage.LongField.RESIDENT_SET_SIZE));
        writeValues(writer, "rchar", traces.getForNode(node, NextflowTraceStorage.LongField.CHARACTERS_READ));
        writeValues(writer, "wchar", traces.getForNode(node, NextflowTraceStorage.LongField.CHARACTERS_WRITTEN));
        writeValues(writer, "realtime", traces.getForNode(node, NextflowTraceStorage.LongField.REALTIME));

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
        if (exitCode != 0) {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read bayes.py stderr", e);
            }
            return;
        }

        MeanStdRecord cpu = null;
        MeanStdRecord mem = null;
        MeanStdRecord read = null;
        MeanStdRecord write = null;
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
        try {
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
            if (cpu == null || mem == null || read == null || write == null) {
                log.error("Online Tarema Scheduler: could not find all estimations in bayes.py output");
                return;
            }
            estimations.put(node, new NodeSpeedEstimation(cpu, mem, read, write));
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read bayes.py stdout", e);
        }
    }

    public boolean recalculateLabels(NextflowTraceStorage traces) {
        return recalculateLabels(traces, traces.getNodes().stream());
    }

    public boolean recalculateLabels(NextflowTraceStorage traces, Stream<NodeWithAlloc> nodesWithNewTraces) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return false;
        }
        nodesWithNewTraces.forEach(node -> runBayesForNode(traces, node));
        return recalculateGroups();
    }

    private boolean recalculateGroups() {
        log.info("Online Tarema Scheduler: Running kmeans.py");
        Process bayesProcess;
        try {
            bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/kmeans.py").start();
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to start kmeans.py process", e);
            return false;
        }
        OutputStream in = bayesProcess.getOutputStream();
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
            exitCode = bayesProcess.waitFor();
            log.info("Online Tarema Scheduler: kmeans.py exited with code {}", exitCode);
        } catch (InterruptedException e) {
            log.error("Online Tarema Scheduler: Failed to wait for kmeans.py to exit", e);
            return false;
        }

        String line;
        if (exitCode != 0) {
            BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: kmeans.py stderr: {}", line);
                }
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to read kmeans.py stderr", e);
            }
            return false;
        }

        boolean cpuGroupsChanged = false;
        boolean memGroupsChanged = false;
        boolean readGroupsChanged = false;
        boolean writeGroupsChanged = false;
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
        try {
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
                } catch (NumberFormatException e) {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED: {}", line);
                    continue;
                }
                if (rowName.equals("maxLabels")) {
                    if (maxLabels == null) {
                        maxLabels = new Labels(cpu, mem, read, write);
                        cpuGroupsChanged = true;
                        memGroupsChanged = true;
                        readGroupsChanged = true;
                        writeGroupsChanged = true;
                    } else {
                        cpuGroupsChanged |= maxLabels.setCpuLabel(cpu);
                        memGroupsChanged |= maxLabels.setMemLabel(mem);
                        readGroupsChanged |= maxLabels.setSequentialReadLabel(read);
                        writeGroupsChanged |= maxLabels.setSequentialWriteLabel(write);
                    }
                } else if (nodesWritten.containsKey(rowName)) {
                    NodeWithAlloc node = nodesWritten.get(rowName);
                    Labels nodeLabels = labels.get(node);
                    if (nodeLabels == null) {
                        nodeLabels = new Labels(cpu, mem, read, write);
                        labels.put(node, nodeLabels);
                    } else {
                        cpuGroupsChanged |= nodeLabels.setCpuLabel(cpu);
                        memGroupsChanged |= nodeLabels.setMemLabel(mem);
                        readGroupsChanged |= nodeLabels.setSequentialReadLabel(read);
                        writeGroupsChanged |= nodeLabels.setSequentialWriteLabel(write);
                    }
                } else {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED {}", rowName);
                }
            }
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read kmeans.py stdout", e);
        }
        return cpuGroupsChanged || memGroupsChanged || readGroupsChanged || writeGroupsChanged;
    }

    public GroupWeights getGroupWeights() {
        if (labels.isEmpty()) {
            log.info("Online Tarema Scheduler: No node labels to calculate task labels from");
            return null;
        }

        float totalCpu = 0;
        float[] cpusPerCpuGroup = new float[maxLabels.getCpuLabel()];
        long totalMemory = 0;
        long[] memoryPerMemGroup = new long[maxLabels.getMemLabel()];
        int totalNodes = 0;
        int[] nodesPerSequentialReadGroup = new int[maxLabels.getSequentialReadLabel()];
        int[] nodesPerSequentialWriteGroup = new int[maxLabels.getSequentialWriteLabel()];

        for (Map.Entry<NodeWithAlloc, Labels> e : labels.entrySet()) {
            NodeWithAlloc node = e.getKey();
            String nodeName = node.getName();
            Labels nodeLabels = e.getValue();

            totalCpu += node.getMaxResources().getCpu().floatValue();
            cpusPerCpuGroup[nodeLabels.getCpuLabel() - 1] += node.getMaxResources().getCpu().floatValue();
            totalMemory += node.getMaxResources().getRam().longValue();
            memoryPerMemGroup[nodeLabels.getMemLabel() - 1] += node.getMaxResources().getRam().longValue();
            totalNodes++;
            nodesPerSequentialReadGroup[nodeLabels.getSequentialReadLabel() - 1]++;
            nodesPerSequentialWriteGroup[nodeLabels.getSequentialWriteLabel() - 1]++;
        }

        float[] cpuGroupWeights = new float[maxLabels.getCpuLabel()];
        for (int i = 0; i < maxLabels.getCpuLabel(); i++) {
            cpuGroupWeights[i] = cpusPerCpuGroup[i] / totalCpu;
        }
        float[] ramGroupWeights = new float[maxLabels.getMemLabel()];
        for (int i = 0; i < maxLabels.getMemLabel(); i++) {
            ramGroupWeights[i] = (float) memoryPerMemGroup[i] / totalMemory;
        }
        float[] readGroupWeights = new float[maxLabels.getSequentialReadLabel()];
        for (int i = 0; i < maxLabels.getSequentialReadLabel(); i++) {
            readGroupWeights[i] = (float) nodesPerSequentialReadGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes reading capabilities?
        }
        float[] writeGroupWeights = new float[maxLabels.getSequentialWriteLabel()];
        for (int i = 0; i < maxLabels.getSequentialReadLabel(); i++) {
            writeGroupWeights[i] = (float) nodesPerSequentialWriteGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes writing capabilities?
        }

        return new GroupWeights(cpuGroupWeights, ramGroupWeights, readGroupWeights, writeGroupWeights);
    }

    public record GroupWeights(float[] cpu, float[] ram, float[] read, float[] write) {
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
            return new MeanStdRecord(Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
        } catch (NumberFormatException e) {
            log.error("Online Tarema Scheduler: Failed to parse mean and std from line \"{}\"", line);
            return null;
        }
    }
}
