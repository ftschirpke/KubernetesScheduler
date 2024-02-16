package labelling;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import lombok.extern.slf4j.Slf4j;

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
            new NodeWithAlloc("local", requirementsHelper(8, 16)),
            new NodeWithAlloc("a1", requirementsHelper(2*4, 32)),
            new NodeWithAlloc("a2", requirementsHelper(2*4, 32)),
            new NodeWithAlloc("n1", requirementsHelper(8, 16)),
            new NodeWithAlloc("n2", requirementsHelper(8, 16)),
            new NodeWithAlloc("c2", requirementsHelper(8, 32))
    };
    static String[] experiments = new String[]{"atacseq", "bacass", "chipseq", "eager", "methylseq"};
    static String[] labels = new String[]{"test", "train-1", "train-2"};

    public static Map<NodeWithAlloc, NodeLabeller.NodeSpeedEstimation> lotaruBenchmarkResults = new HashMap<>(
            Map.of(
                    nodes[0], new NodeLabeller.NodeSpeedEstimation(458, 18700, 414, 415),
                    nodes[1], new NodeLabeller.NodeSpeedEstimation(223,11000,306,301),
                    nodes[2], new NodeLabeller.NodeSpeedEstimation(223,11000,341,336),
                    nodes[3], new NodeLabeller.NodeSpeedEstimation(369,13400,481,483),
                    nodes[4], new NodeLabeller.NodeSpeedEstimation(468,17000,481,483),
                    nodes[5], new NodeLabeller.NodeSpeedEstimation(523,18900,481,483)
            )
    );

    String[] csvHeader;

    Map<NodeWithAlloc, List<String[]>> csvData = new HashMap<>();

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
                        data.add(line.split(","));
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

    static String[] traceValues = new String[]{"Realtime", "%cpu", "cpus","rss", "rchar", "wchar", "read_bytes", "write_bytes", "vmem", "memory", "peak_rss"};
    NextflowTraceRecord taskTraceFromLine(String[] line) {
        NextflowTraceRecord trace = new NextflowTraceRecord();
        for (String valueName : traceValues) {
            String value = getFromLine(valueName, line);
            if (value != null) {
                trace.insertValue(valueName.toLowerCase(), value);
            }
        }
        return trace;
    }

    private String getFromLine(String valueName, String[] line) {
        int index = Arrays.asList(csvHeader).indexOf(valueName);
        return line[index];
    }
}
