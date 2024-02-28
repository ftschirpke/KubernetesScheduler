package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.CentroidCluster;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class NodeFirstLabeller {
    @Getter
    private Labels maxLabels = null;
    @Getter
    private final Map<NodeWithAlloc, Labels> labels = new HashMap<>();
    private final Map<NodeWithAlloc, NodeSpeedEstimation> estimations = new HashMap<>();

    private final SilhouetteScore<LabelledPoint<NodeWithAlloc>> silhouetteScore = new SilhouetteScore<>();

    private final String bayesFile = "external/new-node-first-bayes.py"; // HACK: also acts as a lock for the process
    private Process bayesProcess = null;
    private BufferedReader stdoutReader = null;
    private BufferedReader stderrReader = null;
    private PrintWriter stdinWriter = null;

    public record NodeLabelState(Labels maxLabels, Map<NodeWithAlloc, Labels> labels, GroupWeights groupWeights) {
    }

    public static NodeLabelState labelOnce(Map<NodeWithAlloc, NodeSpeedEstimation> estimations) throws IOException {
        NodeFirstLabeller labeller = new NodeFirstLabeller();
        labeller.estimations.putAll(estimations);
        labeller.recalculateLabelsFromSpeedEstimations();
        Labels maxLabels = labeller.getMaxLabels();
        Map<NodeWithAlloc, Labels> labels = labeller.getLabels();
        GroupWeights groupWeights = GroupWeights.forNodeLabels(maxLabels, labels);
        return new NodeLabelState(maxLabels, labels, groupWeights);
    }

    private void runBayesForNode(NextflowTraceStorage traces, NodeWithAlloc node, List<Integer> ids) {
        log.info("Online Tarema Scheduler: Running bayes.py for node {}", node.getName());
        if (bayesProcess == null) {
            try {
                bayesProcess = new ProcessBuilder("external/venv/bin/python3", bayesFile).start();
            } catch (IOException e) {
                log.error("Online Tarema Scheduler: Failed to start bayes.py process", e);
                return;
            }
            stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
            stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
            stdinWriter = new PrintWriter(new OutputStreamWriter(bayesProcess.getOutputStream()), true);
        }
        synchronized (bayesFile) { // ensure that only one thread is accessing the process at a time
            ids.forEach(id -> stdinWriter.println(traces.asString(id)));

            String line;

            Gaussian cpu = null;
            Gaussian mem = null;
            Gaussian read = null;
            Gaussian write = null;
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
        return recalculateLabelsFromSpeedEstimations();
    }

    private boolean recalculateLabelsFromSpeedEstimations() {
        Map<NodeWithAlloc, Integer> cpuLabels = nodeLabelsForResource(estimation -> estimation.cpu().mean());
        Map<NodeWithAlloc, Integer> memLabels = nodeLabelsForResource(estimation -> estimation.mem().mean());
        Map<NodeWithAlloc, Integer> readLabels = nodeLabelsForResource(estimation -> estimation.read().mean());
        Map<NodeWithAlloc, Integer> writeLabels = nodeLabelsForResource(estimation -> estimation.write().mean());

        int maxCpuLabel = cpuLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxMemLabel = memLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxReadLabel = readLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxWriteLabel = writeLabels.values().stream().max(Integer::compareTo).orElse(0);
        Labels newMaxLabels = new Labels(maxCpuLabel, maxMemLabel, maxReadLabel, maxWriteLabel);

        Map<NodeWithAlloc, Labels> newLabels = estimations.keySet().stream()
                .map(node -> Map.entry(node, new Labels(
                        cpuLabels.get(node),
                        memLabels.get(node),
                        readLabels.get(node),
                        writeLabels.get(node)
                ))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        boolean changed;
        synchronized (labels) {
            changed = !newLabels.equals(labels);
            if (!newLabels.keySet().containsAll(labels.keySet())) {
                log.error("Online Tarema Scheduler: New labels do not contain all nodes; lost nodes");
                labels.clear();
            }
            labels.putAll(newLabels);
            maxLabels = newMaxLabels;
        }
        return changed;
    }

    private Map<NodeWithAlloc, Integer> nodeLabelsForResource(Function<NodeSpeedEstimation, Double> resource) {
        List<LabelledPoint<NodeWithAlloc>> points = estimations.entrySet().stream()
                .map(entry -> new LabelledPoint<>(entry.getKey(), resource.apply(entry.getValue())))
                .toList();
        List<CentroidCluster<LabelledPoint<NodeWithAlloc>>> clusters = silhouetteScore.findBestKmeansClustering(points);
        if (clusters.size() == 1) {
            return estimations.keySet().stream()
                    .map(node -> Map.entry(node, 0))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        clusters.sort(Comparator.comparingDouble(cluster -> cluster.getCenter().getPoint()[0]));
        return IntStream.range(0, clusters.size())
                .boxed()
                .flatMap(i -> clusters.get(i).getPoints().stream()
                        .map(point -> Map.entry(point.getLabel(), i))
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public record NodeSpeedEstimation(Gaussian cpu, Gaussian mem, Gaussian read, Gaussian write) {
        public NodeSpeedEstimation(float cpu, float mem, float read, float write) {
            this(new Gaussian(cpu, 0), new Gaussian(mem, 0), new Gaussian(read, 0), new Gaussian(write, 0));
        }
    }

    private record Gaussian(double mean, double std) {
    }

    private Gaussian meanStdFromString(String s) throws NumberFormatException {
        String[] parts = s.split(",");
        try {
            return new Gaussian(-Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
            // HACK: negative mean to inverse the order (higher mean (slower node) -> lower label)
        } catch (NumberFormatException e) {
            log.error("Online Tarema Scheduler: Failed to parse mean and std from line \"{}\"", s);
            return null;
        }
    }
}
