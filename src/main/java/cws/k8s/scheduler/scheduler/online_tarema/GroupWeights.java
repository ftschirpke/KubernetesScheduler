package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;

import javax.swing.*;
import java.util.Map;

public record GroupWeights(float[] cpu, float[] ram, float[] read, float[] write) {

    public static GroupWeights forNodeLabels(Labels maxLabels, Map<NodeWithAlloc, Labels> labels) {
        if (labels.isEmpty()) {
            return null;
        }

        float totalCpu = 0;
        float[] cpusPerCpuGroup = new float[maxLabels.cpuLabel() + 1];
        long totalMemory = 0;
        long[] memoryPerMemGroup = new long[maxLabels.memLabel() + 1];
        int totalNodes = 0;
        int[] nodesPerSequentialReadGroup = new int[maxLabels.sequentialReadLabel() + 1];
        int[] nodesPerSequentialWriteGroup = new int[maxLabels.sequentialWriteLabel() + 1];

        for (Map.Entry<NodeWithAlloc, Labels> e : labels.entrySet()) {
            NodeWithAlloc node = e.getKey();
            Labels nodeLabels = e.getValue();

            totalCpu += node.getMaxResources().getCpu().floatValue();
            cpusPerCpuGroup[nodeLabels.cpuLabel()] += node.getMaxResources().getCpu().floatValue();
            totalMemory += node.getMaxResources().getRam().longValue();
            memoryPerMemGroup[nodeLabels.memLabel()] += node.getMaxResources().getRam().longValue();
            totalNodes++;
            nodesPerSequentialReadGroup[nodeLabels.sequentialReadLabel()]++;
            nodesPerSequentialWriteGroup[nodeLabels.sequentialWriteLabel()]++;
        }

        float[] cpuGroupWeights = new float[cpusPerCpuGroup.length];
        for (int i = 0; i < cpuGroupWeights.length; i++) {
            cpuGroupWeights[i] = cpusPerCpuGroup[i] / totalCpu;
        }
        float[] ramGroupWeights = new float[memoryPerMemGroup.length];
        for (int i = 0; i < ramGroupWeights.length; i++) {
            ramGroupWeights[i] = (float) memoryPerMemGroup[i] / totalMemory;
        }
        float[] readGroupWeights = new float[nodesPerSequentialReadGroup.length];
        for (int i = 0; i < readGroupWeights.length; i++) {
            readGroupWeights[i] = (float) nodesPerSequentialReadGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes reading capabilities?
        }
        float[] writeGroupWeights = new float[nodesPerSequentialWriteGroup.length];
        for (int i = 0; i < writeGroupWeights.length; i++) {
            writeGroupWeights[i] = (float) nodesPerSequentialWriteGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes writing capabilities?
        }

        return new GroupWeights(cpuGroupWeights, ramGroupWeights, readGroupWeights, writeGroupWeights);
    }

    public static GroupWeights forTaskLabels(Labels maxLabels, Map<String, Labels> labels){
        if (labels.isEmpty()) {
            return null;
        }

        int totalTasks = 0;
        int[] tasksPerCpuGroup = new int[maxLabels.cpuLabel() + 1];
        int[] tasksPerMemGroup = new int[maxLabels.memLabel() + 1];
        int[] tasksPerSequentialReadGroup = new int[maxLabels.sequentialReadLabel() + 1];
        int[] tasksPerSequentialWriteGroup = new int[maxLabels.sequentialWriteLabel() + 1];

        for (Map.Entry<String, Labels> e : labels.entrySet()) {
            Labels taskLabels = e.getValue();

            totalTasks++;
            tasksPerCpuGroup[taskLabels.cpuLabel()]++;
            tasksPerMemGroup[taskLabels.memLabel()]++;
            tasksPerSequentialReadGroup[taskLabels.sequentialReadLabel()]++;
            tasksPerSequentialWriteGroup[taskLabels.sequentialWriteLabel()]++;
            // TODO: should tasks be weighted differently based on some metric?
        }

        float[] cpuGroupWeights = new float[tasksPerCpuGroup.length];
        for (int i = 0; i < cpuGroupWeights.length; i++) {
            cpuGroupWeights[i] = (float) tasksPerCpuGroup[i] / totalTasks;
        }
        float[] ramGroupWeights = new float[tasksPerMemGroup.length];
        for (int i = 0; i < ramGroupWeights.length; i++) {
            ramGroupWeights[i] = (float) tasksPerMemGroup[i] / totalTasks;
        }
        float[] readGroupWeights = new float[tasksPerSequentialReadGroup.length];
        for (int i = 0; i < readGroupWeights.length; i++) {
            readGroupWeights[i] = (float) tasksPerSequentialReadGroup[i] / totalTasks;
        }
        float[] writeGroupWeights = new float[tasksPerSequentialWriteGroup.length];
        for (int i = 0; i < writeGroupWeights.length; i++) {
            writeGroupWeights[i] = (float) tasksPerSequentialWriteGroup[i] / totalTasks;
        }

        return new GroupWeights(cpuGroupWeights, ramGroupWeights, readGroupWeights, writeGroupWeights);
    }
}
