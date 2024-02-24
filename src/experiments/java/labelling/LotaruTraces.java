package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class LotaruTraces {
    static Requirements requirementsHelper(int cpus, int gigabytesMemory) {
        BigDecimal cpu = BigDecimal.valueOf(cpus);
        BigDecimal mem = BigDecimal.valueOf((long) gigabytesMemory * 1024 * 1024 * 1024);
        return new Requirements(cpu, mem);
    }

    static NodeWithAlloc[] nodes = new NodeWithAlloc[]{
            new NodeWithAlloc("local", requirementsHelper(8, 16)), // TODO: should "local" be included?
            new NodeWithAlloc("a1", requirementsHelper(2 * 4, 32)),
            new NodeWithAlloc("a2", requirementsHelper(2 * 4, 32)),
            new NodeWithAlloc("n1", requirementsHelper(8, 16)),
            new NodeWithAlloc("n2", requirementsHelper(8, 16)),
            new NodeWithAlloc("c2", requirementsHelper(8, 32))
    };
    public static final Map<String, NodeWithAlloc> machineNames = Map.of(
            "local", nodes[0], // TODO: should "local" be included?
            "asok01", nodes[1],
            "asok02", nodes[2],
            "n1", nodes[3],
            "n2", nodes[4],
            "c2", nodes[5]
    );
    static String[] experiments = new String[]{"atacseq", "bacass", "chipseq", "eager", "methylseq"};
    static String[] labels = new String[]{"test", "train-1", "train-2"};

    public static Map<NodeWithAlloc, NodeFirstLabeller.NodeSpeedEstimation> lotaruBenchmarkResults = new HashMap<>(
            Map.of(
                    nodes[0], new NodeFirstLabeller.NodeSpeedEstimation(458, 18700, 414, 415), // TODO: should "local" be included?
                    nodes[1], new NodeFirstLabeller.NodeSpeedEstimation(223, 11000, 306, 301),
                    nodes[2], new NodeFirstLabeller.NodeSpeedEstimation(223, 11000, 341, 336),
                    nodes[3], new NodeFirstLabeller.NodeSpeedEstimation(369, 13400, 481, 483),
                    nodes[4], new NodeFirstLabeller.NodeSpeedEstimation(468, 17000, 481, 483),
                    nodes[5], new NodeFirstLabeller.NodeSpeedEstimation(523, 18900, 481, 483)
            )
    );

    String[] csvHeader;

    Map<NodeWithAlloc, List<String[]>> csvData = new HashMap<>();
    List<String> taskNames = new ArrayList<>();

    private Stream<Map.Entry<NodeWithAlloc, String[]>> allLineEntries() {
        return csvData.entrySet().stream().flatMap(entry -> {
            NodeWithAlloc node = entry.getKey();
            List<String[]> lines = entry.getValue();
            return lines.stream().map(line -> Map.entry(node, line));
        });
    }

    private Stream<String[]> allLines() {
        return allLineEntries().map(Map.Entry::getValue);
    }

    Stream<String[]> allLinesByNode() {
        return allLineEntries().sorted(Comparator.comparing(entry -> {
            NodeWithAlloc node = entry.getKey();
            return ArrayUtils.indexOf(nodes, node);
        })).map(Map.Entry::getValue);
    }

    Stream<String[]> allLinesByTask() {
        return allLines().sorted(Comparator.comparing(line -> {
            String task = getFromLine("Task", line);
            return taskNames.indexOf(task);
        }));
    }

    Stream<String[]> allLinesFairly() {
        return allLineEntries().sorted(Comparator.comparing(entry -> {
            NodeWithAlloc node = entry.getKey();
            int nodeIndex = ArrayUtils.indexOf(nodes, node);
            String[] line = entry.getValue();
            int indexInNode = csvData.get(node).indexOf(line);
            return indexInNode * nodes.length + nodeIndex;
        })).map(Map.Entry::getValue);
    }

    String[] getLineForTask(NodeWithAlloc node, String taskName) {
        if (node == null) {
            int idx = RandomUtils.nextInt(0, nodes.length);
            node = nodes[idx];
        }
        if (taskName == null) {
            int idx = RandomUtils.nextInt(0, taskNames.size());
            taskName = taskNames.get(idx);
        }
        List<String[]> nodeData = csvData.get(node);
        if (nodeData == null) {
            return null;
        }
        for (int i = 0; i < nodeData.size(); i++) {
            String[] row = nodeData.get(i);
            if (getFromLine("Task", row).equals(taskName)) {
                return nodeData.remove(i);
            }
        }
        return null;
    }

    void status() {
        System.out.println(Arrays.toString(csvHeader));
        Map<String, Map<String, Integer>> combinationCounts = new HashMap<>();
        for (Map.Entry<NodeWithAlloc, List<String[]>> entry : csvData.entrySet()) {
            System.out.println("=== " + entry.getKey().getName() + " ===");
            for (String[] row : entry.getValue()) {
                String label = row[0];
                combinationCounts.putIfAbsent(label, new HashMap<>());
                Map<String, Integer> inner = combinationCounts.get(label);
                String task = row[4];
                inner.putIfAbsent(task, 0);
                inner.put(task, inner.get(task) + 1);
                System.out.println(Arrays.toString(row));
            }
        }
        System.out.println("Finished reading data.");
        for (Map.Entry<String, Map<String, Integer>> entry : combinationCounts.entrySet()) {
            String label = entry.getKey();
            for (Map.Entry<String, Integer> inner : entry.getValue().entrySet()) {
                System.out.println("(" + label + ", " + inner.getKey() + "): " + inner.getValue());
            }
        }
    }

    void readTraces(File tracesDirectory, String experimentName, String experimentLabel) {
        if (!tracesDirectory.isDirectory()) {
            log.error("{} is not a directory", tracesDirectory);
            System.exit(1);
        }

        File[] nodeDirs = tracesDirectory.listFiles();
        if (nodeDirs == null) {
            log.error("No files in {}", tracesDirectory);
            System.exit(1);
        }
        for (File nodeDir : nodeDirs) {
            String nodeName = nodeDir.getName();
            Stream<NodeWithAlloc> nodeStream = Arrays.stream(nodes);
            Optional<NodeWithAlloc> nodeOptional = nodeStream.filter(n -> n.getName().equals(nodeName)).findFirst();
            if (nodeOptional.isEmpty()) {
                log.info("Skipping directory {} as it is not in the list of nodes", nodeName);
                continue;
            }
            NodeWithAlloc node = nodeOptional.get();
            if (nodeDir.isDirectory()) {
                File[] experimentDirs = nodeDir.listFiles();
                if (experimentDirs == null) {
                    log.error("No files in {}", nodeDir);
                    System.exit(1);
                }
                File experimentDir = new File(nodeDir, "results_" + experimentName);
                if (!experimentDir.isDirectory()) {
                    log.error("Experiment {} not found in {}", experimentName, nodeDir);
                    System.exit(1);
                }
                File[] traceFiles = experimentDir.listFiles();
                if (traceFiles == null || traceFiles.length != 1) {
                    log.error("Expected exactly one file in {}", experimentDir);
                    System.exit(1);
                }
                File traceFile = traceFiles[0];
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(traceFile));
                    String line;
                    line = reader.readLine();
                    if (line == null) {
                        log.error("Empty file {}", traceFile.getName());
                        System.exit(1);
                    }
                    String[] header = line.split(",");
                    if (csvHeader == null) {
                        csvHeader = header;
                    } else if (!Arrays.equals(csvHeader, header)) {
                        log.error("Header mismatch in {}", traceFile.getName());
                        System.exit(1);
                    }
                    List<String[]> data = new ArrayList<>();
                    while ((line = reader.readLine()) != null) {
                        if (!line.startsWith(experimentLabel)) {
                            continue;
                        }
                        String[] dataLine = line.split(",");
                        String taskName = getFromLine("Task", dataLine);
                        if (!taskNames.contains(taskName)) {
                            taskNames.add(taskName);
                        }
                        data.add(dataLine);
                    }
                    if (csvData.containsKey(node)) {
                        log.error("Duplicate data for {}", node);
                        System.exit(1);
                    }
                    csvData.put(node, data);
                } catch (IOException e) {
                    log.error("Error reading file {}", traceFile.getName());
                    System.exit(1);
                }
            }
        }
    }

    TaskConfig taskConfigFromLine(String[] line) {
        return new TaskConfig(
                getFromLine("Task", line),
                Float.parseFloat(getFromLine("%cpu", line)),
                Long.parseLong(getFromLine("memory", line))
        );
    }

    static String[] traceValues = new String[]{"Realtime", "%cpu", "cpus", "rss", "rchar", "wchar", "read_bytes", "write_bytes", "vmem", "memory", "peak_rss"};
    static String[] zeroValues = new String[]{"%mem", "syscr", "syscw", "vol_ctxt", "inv_ctxt", "peak_vmem"};

    NextflowTraceRecord taskTraceFromLine(String[] line) {
        NextflowTraceRecord trace = new NextflowTraceRecord();
        for (String valueName : traceValues) {
            String value = getFromLine(valueName, line);
            if (value != null) {
                trace.insertValue(valueName.toLowerCase(), value);
            }
        }
        for (String valueName : zeroValues) {
            trace.insertValue(valueName.toLowerCase(), "0");
        }
        return trace;
    }

    String getFromLine(String valueName, String[] line) {
        int index = ArrayUtils.indexOf(csvHeader, valueName);
        return line[index];
    }
}
