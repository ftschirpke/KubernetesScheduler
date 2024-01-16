package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

@Getter
@Slf4j
public class TaskLabeller {

    private final int labelSpaceSize;
    private final HashMap<String, Labels> labels;

    public TaskLabeller(int labelSpaceSize) {
        this.labelSpaceSize = labelSpaceSize;
        this.labels = new HashMap<>();
    }

    public void recalculateLabels(NextflowTraceStorage traces,
                                  float[] cpuGroupWeights,
                                  float[] ramGroupWeights,
                                  float[] readGroupWeights,
                                  float[] writeGroupWeights) throws NoSuchElementException {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return;
        }
        List<Float> allCpuPercentages = traces.getAll(NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
        double minCpuPercentage = allCpuPercentages.stream().mapToDouble(Float::doubleValue).min().orElseThrow();
        double maxCpuPercentage = allCpuPercentages.stream().mapToDouble(Float::doubleValue).max().orElseThrow();
        Percentiles cpuPercentiles = new Percentiles(minCpuPercentage, maxCpuPercentage, cpuGroupWeights);

        List<Float> allMemoryPercentages = traces.getAll(NextflowTraceStorage.FloatField.MEMORY_PERCENTAGE);
        double minMemoryPercentage = allMemoryPercentages.stream().mapToDouble(Float::doubleValue).min().orElseThrow();
        double maxMemoryPercentage = allMemoryPercentages.stream().mapToDouble(Float::doubleValue).max().orElseThrow();
        Percentiles memoryPercentiles = new Percentiles(minMemoryPercentage, maxMemoryPercentage, ramGroupWeights);

        // TODO: sequential read and write

        for (String abstractTaskName : traces.getAbstractTaskNames()) {
            Stream<Float> cpuValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
            double avgCpuPercentage = cpuValues.mapToDouble(Float::doubleValue).average().orElseThrow();
            int cpuLabel = cpuPercentiles.percentileNumber(avgCpuPercentage);

            Stream<Float> memoryValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.FloatField.MEMORY_PERCENTAGE);
            double avgMemoryPercentage = memoryValues.mapToDouble(Float::doubleValue).average().orElseThrow();
            int ramLabel = memoryPercentiles.percentileNumber(avgMemoryPercentage);

            // TODO: sequential read and write
            int sequentialReadLabel = 1;
            int sequentialWriteLabel = 1;

            labels.put(abstractTaskName, new Labels(labelSpaceSize, cpuLabel, ramLabel, sequentialReadLabel, sequentialWriteLabel));
        }
    }

    private static class Percentiles {
        private final double minValue;
        private final double range;
        private final int segments;
        private final float[] weights;

        public Percentiles(double minValue, double maxValue, float[] weights ) {
            this.minValue = minValue;
            this.range = maxValue - minValue;
            this.segments = weights.length;
            this.weights = weights;
        }

        public int percentileNumber(double value) {
            float accumulatedWeight = 0;
            if (value < minValue) {
                log.warn("Unexpected value: {} is below the minimum value {}.", value, minValue);
            }
            for (int i = 0; i < segments; i++) {
                accumulatedWeight += weights[i];
                if (value <= minValue + range * accumulatedWeight) {
                    return i + 1;
                }
            }
            log.warn("Unexpected value: {} is above the maximum value {}.", value, minValue + range);
            return segments;
        }


    }
}
