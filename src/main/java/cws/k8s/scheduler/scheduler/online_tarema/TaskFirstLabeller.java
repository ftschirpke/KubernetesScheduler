package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static cws.k8s.scheduler.scheduler.online_tarema.MyStreamUtils.*;

@Getter
@Slf4j
public class TaskFirstLabeller {
    private Labels maxLabels = null;
    private final Map<String, Labels> labels = new HashMap<>();

    public boolean recalculateLabels(NextflowTraceStorage traces) {
        if (traces.empty()) {
            log.info("No traces to calculate task labels from");
            return false;
        }
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

        List<String> tasksWritten = new ArrayList<>();
        // writer.write("task,cpu1,cpu2,ram1,ram2,read,write\n");
        for (String taskName : traces.getAbstractTaskNames()) {
            writer.write(taskName);
            writer.write(",");
            // Stream<Float> cpuValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
            // writer.write(String.valueOf(floatAvg(cpuValues)));
            // writer.write(",");
            Stream<Float> cpuPercValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
            writer.write(String.valueOf(floatAverage(cpuPercValues)));
            writer.write(",");
            Stream<Long> rssValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.LongField.RESIDENT_SET_SIZE);
            writer.write(String.valueOf(longAverage(rssValues)));
            writer.write(",");
            // Stream<Long> vmemValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.LongField.VIRTUAL_MEMORY);
            // writer.write(String.valueOf(longAvg(vmemValues)));
            // writer.write(",");
            Stream<Long> rCharValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.LongField.CHARACTERS_READ);
            writer.write(String.valueOf(longAverage(rCharValues)));
            writer.write(",");
            Stream<Long> wCharValues = traces.getForAbstractTask(taskName, NextflowTraceStorage.LongField.CHARACTERS_WRITTEN);
            writer.write(String.valueOf(longAverage(wCharValues)));
            writer.write("\n");

            tasksWritten.add(taskName);
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
            Map<String, Labels> newTaskLabels = new HashMap<>();
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
                    if (!maxLabels.equals(newLabels)) {
                        newMaxLabels = newLabels;
                    }
                } else if (tasksWritten.contains(rowName)) {
                    Labels taskLabels = labels.get(rowName);
                    if (!taskLabels.equals(newLabels)) {
                        newTaskLabels.put(rowName, newLabels);
                    }
                } else {
                    log.error("Online Tarema Scheduler: kmeans.py UNEXPECTED {}", rowName);
                }
            }
            if (newMaxLabels != null) {
                maxLabels = newMaxLabels;
                labelsChanged = true;
            }
            if (!newTaskLabels.isEmpty()) {
                labels.putAll(newTaskLabels);
                labelsChanged = true;
            }
        } catch (IOException e) {
            log.error("Online Tarema Scheduler: Failed to read kmeans.py stdout", e);
        }
        return labelsChanged;
    }

}
