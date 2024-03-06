package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import cws.k8s.scheduler.model.NodeWithAlloc;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.*;

@Slf4j
public class PythonNodeEstimator implements NodeEstimator {

    private final BufferedReader stdoutReader;
    private final BufferedReader stderrReader;
    private final PrintWriter stdinWriter;
    private int estimationsCounter = 0;
    private final Set<NodeWithAlloc> nodes;

    public PythonNodeEstimator(String pythonScriptPath, Set<NodeWithAlloc> nodes) {
        this.nodes = nodes;
        try {
            Process pythonProcess = new ProcessBuilder("external/venv/bin/python3", pythonScriptPath).start();
            this.stdoutReader = new BufferedReader(new InputStreamReader(pythonProcess.getInputStream()));
            this.stderrReader = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));
            this.stdinWriter = new PrintWriter(new OutputStreamWriter(pythonProcess.getOutputStream()), true);
        } catch (IOException e) {
            log.error("failed to start python process", e);
            throw new RuntimeException(e);
        }
    }

    synchronized public <T extends Number> void addDataPoint(NodeWithAlloc node, String taskName,
                                                             long rchar, T targetValue) {
        if (!nodes.contains(node)) {
            log.error("Ignore attempt to add data point for unknown node: {}", node);
            return;
        }
        stdinWriter.println(String.format(
                "{\"node\": \"%s\", \"task\": \"%s\", \"rchar\": %d, \"target\": %s}",
                node.getName(), taskName, rchar, targetValue.toString(), nodes.size()
        ));
    }

    synchronized public Map<NodeWithAlloc, Double> estimations() {
        stdinWriter.println(String.format("{\"estimate\": %d, \"id\": %d}", nodes.size(), estimationsCounter));
        estimationsCounter++;
        String line;
        try {
            while (true) {
                line = stdoutReader.readLine();
                if (line == null) {
                    log.error("estimator process stdout closed unexpectedly");
                    while ((line = stderrReader.readLine()) != null) {
                        log.error("estimator process stderr: {}", line);
                    }
                    return null;
                }
                if (line.startsWith("DEBUG")) {
                    log.info("estimator process {}", line);
                } else {
                    break;
                }
            }
            String[] parts = line.split(";");
            if (parts.length < 2) {
                log.error("estimator process returned invalid output: {}", line);
                return null;
            }
            if (!parts[0].equals(Integer.toString(estimationsCounter - 1))) {
                log.error("estimator process returned invalid output: {}", line);
                return null;
            }
            if (Objects.equals(parts[1], "NOT READY")) {
                return null;
            }

            Map<NodeWithAlloc, Double> estimations = new HashMap<>();
            for (int i = 1; i < parts.length; i++) {
                String[] nodeEstimation = parts[i].split("=");
                if (nodeEstimation.length != 2) {
                    log.error("estimator process returned invalid output: {}", line);
                    return null;
                }
                try {
                    String nodeName = nodeEstimation[0];
                    Optional<NodeWithAlloc> optionalNode = nodes.stream()
                            .filter(n -> n.getName().equals(nodeName))
                            .findFirst();
                    if (optionalNode.isEmpty()) {
                        log.error("estimator process returned invalid output: {}", line);
                        return null;
                    }
                    NodeWithAlloc node = optionalNode.get();
                    Double estimation = Double.parseDouble(nodeEstimation[1]);
                    estimations.put(node, estimation);
                } catch (NumberFormatException e) {
                    log.error("estimator process returned invalid output: {}", line);
                    return null;
                }
            }
            return estimations;
        } catch (IOException e) {
            log.error("estimator failed to read process stdout", e);
            return null;
        }
    }
}
