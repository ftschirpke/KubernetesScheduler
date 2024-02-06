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
    private final Map<String, Labels> labels;
    private final Map<String, NodeSpeedEstimation> estimations;

    public NodeLabeller() {
        this.maxLabels = null;
        this.labels = new HashMap<>();
        this.estimations = new HashMap<>();
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
            estimations.put(node.getName(), new NodeSpeedEstimation(cpu, mem, read, write));
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read bayes.py stdout", e);
        }
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

        for (Map.Entry<String, NodeSpeedEstimation> entry : estimations.entrySet()) {
            String nodeName = entry.getKey();
            NodeSpeedEstimation estimation = entry.getValue();
            writeValues(writer, nodeName, estimation.means());
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
                    synchronized (maxLabels) {
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
                    }
                } else if (estimations.containsKey(rowName)) {
                    synchronized (labels) {
                        Labels nodeLabels = labels.get(rowName);
                        if (nodeLabels == null) {
                            nodeLabels = new Labels(cpu, mem, read, write);
                            labels.put(rowName, nodeLabels);
                        } else {
                            cpuGroupsChanged |= nodeLabels.setCpuLabel(cpu);
                            memGroupsChanged |= nodeLabels.setMemLabel(mem);
                            readGroupsChanged |= nodeLabels.setSequentialReadLabel(read);
                            writeGroupsChanged |= nodeLabels.setSequentialWriteLabel(write);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read kmeans.py stdout", e);
        }
        return cpuGroupsChanged || memGroupsChanged || readGroupsChanged || writeGroupsChanged;
    }

    private <T> void writeValues(PrintWriter writer, String name, Stream<T> values) {
        writer.write(name);
        values.forEachOrdered(val -> writer.write("," + val));
        writer.write("\n");
    }

    private record NodeSpeedEstimation(MeanStdRecord cpu, MeanStdRecord mem, MeanStdRecord read, MeanStdRecord write) {
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
