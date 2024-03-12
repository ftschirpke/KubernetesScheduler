package cws.k8s.scheduler.scheduler.online_tarema;

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
        if (workDir == null) {
            log.error("Work directory for LabelsLogger is null");
            nodeLabelsFile = null;
            nodeEstimationsFile = null;
            taskLabelsFile = null;
            return;
        }
        File workDirFile = new File(workDir);
        if (!workDirFile.exists()) {
            boolean dirCreated = workDirFile.mkdirs();
            if (!dirCreated) {
                log.error("Failed to create work directory: {}", workDir);
                nodeLabelsFile = null;
                nodeEstimationsFile = null;
                taskLabelsFile = null;
                return;
            }
        }

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
    private synchronized boolean writeToFile(String content, File file) {
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

    private <T> void writeMap(Map<String, T> labels, String target, int iteration, File file) {
        if (file == null) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"iteration\": ");
        sb.append(iteration);
        sb.append(", ");
        for (Map.Entry<String, T> entry : labels.entrySet()) {
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

        if (!writeToFile(content, file)) {
            log.error("Failed to write node labels to file: {}", content);
        }

    }

    public void writeNodeLabels(Map<String, Integer> labels, String target, int iteration) {
        writeMap(labels, target, iteration, nodeLabelsFile);
    }

    public void writeNodeEstimations(Map<String, Double> estimations, String target, int iteration) {
        writeMap(estimations, target, iteration, nodeEstimationsFile);
    }

    public void writeTaskLabels(Map<String, Integer> labels, String target, int iteration) {
        writeMap(labels, target, iteration, taskLabelsFile);
    }


}
