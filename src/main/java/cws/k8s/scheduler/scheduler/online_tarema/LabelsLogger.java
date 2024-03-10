package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Map;

@Slf4j
public class LabelsLogger {

    static final String NODE_LABELS_FILE = "node_labels.csv";
    static final String NODE_ESTIMATIONS_FILE = "node_estimations.csv";
    static final String TASK_LABELS_FILE = "task_labels.csv";

    private final File nodeLabelsFile;
    private final File nodeEstimationsFile;
    private final File taskLabelsFile;

    public LabelsLogger(String workDir) {
        this.nodeLabelsFile = new File(String.format("%s/%s", workDir, NODE_LABELS_FILE));
        if (nodeLabelsFile.exists()) {
            nodeLabelsFile.delete();
        }
        this.nodeEstimationsFile = new File(String.format("%s/%s", workDir, NODE_ESTIMATIONS_FILE));
        if (nodeEstimationsFile.exists()) {
            nodeEstimationsFile.delete();
        }
        this.taskLabelsFile = new File(String.format("%s/%s", workDir, TASK_LABELS_FILE));
        if (taskLabelsFile.exists()) {
            taskLabelsFile.delete();
        }
    }

    // Writes content to a file in the work directory and returns true if it was successful.
    private boolean writeToFile(String content, File file) {
        if (!file.exists()) {
            if (file.getParentFile() != null && !file.getParentFile().exists()) {
                boolean parentsCreated = file.getParentFile().mkdirs();
                if (!parentsCreated) {
                    System.out.println("Failed to create parent directories for file: " + file);
                    return false;
                }
            }
            try {
                boolean fileCreated = file.createNewFile();
                if (!fileCreated) {
                    System.out.println("Failed to create file: " + file);
                    return false;
                }
            } catch (IOException e) {
                System.out.println("Threw exception while creating file: " + file);
                return false;
            }
        }

        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file, true))) {
            writer.println(content);
        } catch (FileNotFoundException e) {
            System.out.println("Exception while writing to: " + file);
            return false;
        }
        return true;
    }

    public void writeNodeLabels(Map<NodeWithAlloc, Integer> labels, String target, int iteration) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"iteration\": ");
        sb.append(iteration);
        sb.append(", ");
        for (Map.Entry<NodeWithAlloc, Integer> entry : labels.entrySet()) {
            sb.append('\"');
            sb.append(entry.getKey().getName());
            sb.append("\": ");
            sb.append(entry.getValue());
            sb.append(", ");
        }
        sb.append("\"target\": \"");
        sb.append(target);
        sb.append("\"}");
        String content = sb.toString();

        if (!writeToFile(content, nodeLabelsFile)) {
            log.error("Failed to write node labels to file: {}", content);
        }
    }

    public void writeNodeEstimations(Map<NodeWithAlloc, Double> estimations, String target, int iteration) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"iteration\": ");
        sb.append(iteration);
        sb.append(", ");
        for (Map.Entry<NodeWithAlloc, Double> entry : estimations.entrySet()) {
            sb.append('\"');
            sb.append(entry.getKey().getName());
            sb.append("\": ");
            sb.append(entry.getValue());
            sb.append(", ");
        }
        sb.append("\"target\": \"");
        sb.append(target);
        sb.append("\"}");
        String content = sb.toString();

        if (!writeToFile(content, nodeEstimationsFile)) {
            log.error("Failed to write node estimations to file: {}", content);
        }
    }

    public void writeTaskLabels(Map<String, Integer> labels, String target, int iteration) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"iteration\": ");
        sb.append(iteration);
        sb.append(", ");
        for (Map.Entry<String, Integer> entry : labels.entrySet()) {
            sb.append('\"');
            sb.append(entry.getKey());
            sb.append("\": ");
            sb.append(entry.getValue());
            sb.append(", ");
        }
        sb.append("\"target\": \"");
        sb.append(target);
        sb.append("\"}");
        String content = sb.toString();

        if (!writeToFile(content, taskLabelsFile)) {
            log.error("Failed to write task labels to file: {}", content);
        }
    }


}
