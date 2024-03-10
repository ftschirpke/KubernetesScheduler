package labelling;

import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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

    private static final String[] nodes = new String[]{"local", "a1", "a2", "n1", "n2", "c2"};
    public static Map<String, Float> nodeCpus = new HashMap<>(
            Map.of(
                    nodes[0], 8f,
                    nodes[1], 2*4f,
                    nodes[2], 2*4f,
                    nodes[3], 8f,
                    nodes[4], 8f,
                    nodes[5], 8f
            )
    );
    public static Map<String, Float> nodeGigabyteMemory = new HashMap<>(
            Map.of(
                    nodes[0], 16f,
                    nodes[1], 32f,
                    nodes[2], 32f,
                    nodes[3], 16f,
                    nodes[4], 16f,
                    nodes[5], 32f
            )
    );

    public static List<String> getNodesWithoutLocal() {
        return Arrays.asList(nodes).subList(1, nodes.length);
    }

    public static String[] getNodesIncludingLocal() {
        return nodes;
    }


    public static final Map<String, String> machineNames = Map.of(
            "local", nodes[0],
            "asok01", nodes[1],
            "asok02", nodes[2],
            "n1", nodes[3],
            "n2", nodes[4],
            "c2", nodes[5]
    );

    static String[] experiments = new String[]{"atacseq", "bacass", "chipseq", "eager", "methylseq"};
    static String[] labels = new String[]{"test", "train-1", "train-2"};

    // TODO: should "local" be included?
    public static Map<String, Double> cpuBenchmarks = new HashMap<>(
            Map.of(
                    nodes[0], 458.0,
                    nodes[1], 223.0,
                    nodes[2], 223.0,
                    nodes[3], 369.0,
                    nodes[4], 468.0,
                    nodes[5], 523.0
            )
    );
    public static Map<String, Double> memoryBenchmarks = new HashMap<>(
            Map.of(
                    nodes[0], 18700.0,
                    nodes[1], 11000.0,
                    nodes[2], 11000.0,
                    nodes[3], 13400.0,
                    nodes[4], 17000.0,
                    nodes[5], 18900.0
            )
    );
    public static Map<String, Double> readBenchmarks = new HashMap<>(
            Map.of(
                    nodes[0], 414.0,
                    nodes[1], 306.0,
                    nodes[2], 341.0,
                    nodes[3], 481.0,
                    nodes[4], 481.0,
                    nodes[5], 481.0
            )
    );
    public static Map<String, Double> writeBenchmarks = new HashMap<>(
            Map.of(
                    nodes[0], 415.0,
                    nodes[1], 301.0,
                    nodes[2], 336.0,
                    nodes[3], 483.0,
                    nodes[4], 483.0,
                    nodes[5], 483.0
            )
    );

    String[] csvHeader;

    Map<String, List<String[]>> csvData = new HashMap<>();
    List<String> taskNames = new ArrayList<>();

    private Stream<Map.Entry<String, String[]>> allLineEntries(boolean includeLocal) {
        Stream<Map.Entry<String, List<String[]>>> stream = csvData.entrySet().stream();
        if (!includeLocal) {
            stream = stream.filter(entry -> !entry.getKey().equals("local"));
        }
        return stream.flatMap(entry -> {
            String nodeName = entry.getKey();
            List<String[]> lines = entry.getValue();
            return lines.stream().map(line -> Map.entry(nodeName, line));
        });
    }

    private Stream<String[]> allLines(boolean includeLocal) {
        return allLineEntries(includeLocal).map(Map.Entry::getValue);
    }

    Stream<String[]> allLinesByNode(boolean includeLocal) {
        return allLineEntries(includeLocal).sorted(Comparator.comparing(entry -> {
            String nodeName = entry.getKey();
            return ArrayUtils.indexOf(nodes, nodeName);
        })).map(Map.Entry::getValue);
    }

    Stream<String[]> allLinesByTask(boolean includeLocal) {
        return allLines(includeLocal).sorted(Comparator.comparing(line -> {
            String task = getFromLine("Task", line);
            return taskNames.indexOf(task);
        }));
    }

    Stream<String[]> allLinesFairly(boolean includeLocal) {
        return allLineEntries(includeLocal).sorted(Comparator.comparing(entry -> {
            String nodeName = entry.getKey();
            int nodeIndex = ArrayUtils.indexOf(nodes, nodeName);
            String[] line = entry.getValue();
            int indexInNode = csvData.get(nodeName).indexOf(line);
            return indexInNode * nodes.length + nodeIndex;
        })).map(Map.Entry::getValue);
    }

    String[] getLineForTask(String nodeName, String taskName) {
        if (nodeName == null) {
            int idx = RandomUtils.nextInt(0, nodes.length);
            nodeName = nodes[idx];
        }
        if (taskName == null) {
            int idx = RandomUtils.nextInt(0, taskNames.size());
            taskName = taskNames.get(idx);
        }
        List<String[]> nodeData = csvData.get(nodeName);
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
        for (Map.Entry<String, List<String[]>> entry : csvData.entrySet()) {
            System.out.println("=== " + entry.getKey() + " ===");
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
            int index = ArrayUtils.indexOf(nodes, nodeName);
            if (index == -1) {
                log.info("Skipping directory {} as it is not in the list of nodes", nodeName);
                continue;
            }
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
                    if (csvData.containsKey(nodeName)) {
                        log.error("Duplicate data for {}", nodeName);
                        System.exit(1);
                    }
                    csvData.put(nodeName, data);
                } catch (IOException e) {
                    log.error("Error reading file {}", traceFile.getName());
                    System.exit(1);
                }
            }
        }
        // sort each node's data by task name
        for (String nodeName : nodes) {
            if (!csvData.containsKey(nodeName)) {
                log.error("No data for {}", nodeName);
                System.exit(1);
            }
            List<String[]> data = csvData.get(nodeName);
            data.sort(Comparator.comparing(line -> {
                String task = getFromLine("Task", line);
                return taskNames.indexOf(task);
            }));
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

    TraceRecord taskTraceFromLine(String[] line) {
        TraceRecord trace = new TraceRecord();
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
