package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@Getter
@Slf4j
public class TaskLabeller {

    private final HashMap<String, Labels> labels;

    public TaskLabeller() {
        this.labels = new HashMap<>();
    }

    /**
     * Recalculates the labels for all tasks based on the historic traces and weights for each label group.
     */
    public void recalculateLabels(NextflowTraceStorage traces,
                                  float[] cpuGroupWeights,
                                  float[] ramGroupWeights,
                                  float[] readGroupWeights,
                                  float[] writeGroupWeights) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return;
        }
        List<Float> allCpuPercentages = traces.getAll(NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
        Percentiles cpuPercentiles = new Percentiles(floatMin(allCpuPercentages), floatMax(allCpuPercentages), cpuGroupWeights);
        List<Long> allRssValues = traces.getAll(NextflowTraceStorage.LongField.RESIDENT_SET_SIZE);
        Percentiles memoryPercentiles = new Percentiles(longMin(allRssValues), longMax(allRssValues), ramGroupWeights);
        List<Long> allRCharValues = traces.getAll(NextflowTraceStorage.LongField.CHARACTERS_READ);
        Percentiles readPercentiles = new Percentiles(longMin(allRCharValues), longMax(allRCharValues), readGroupWeights);
        List<Long> allWCharValues = traces.getAll(NextflowTraceStorage.LongField.CHARACTERS_WRITTEN);
        Percentiles writePercentiles = new Percentiles(longMin(allWCharValues), longMax(allWCharValues), writeGroupWeights);

        for (String abstractTaskName : traces.getAbstractTaskNames()) {
            Stream<Float> cpuValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.FloatField.CPU_PERCENTAGE);
            double avgCpuPercentage = cpuValues.mapToDouble(Float::doubleValue).average().orElseThrow();
            int cpuLabel = cpuPercentiles.percentileNumber(avgCpuPercentage);

            Stream<Long> rssValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.LongField.RESIDENT_SET_SIZE);
            double avgRss = rssValues.mapToLong(Long::longValue).average().orElseThrow();
            int memoryLabel = memoryPercentiles.percentileNumber(avgRss);

            Stream<Long> rCharValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.LongField.CHARACTERS_READ);
            double avgRChar = rCharValues.mapToLong(Long::longValue).average().orElseThrow();
            int sequentialReadLabel = readPercentiles.percentileNumber(avgRChar);

            Stream<Long> wCharValues = traces.getForAbstractTask(abstractTaskName, NextflowTraceStorage.LongField.CHARACTERS_WRITTEN);
            double avgWChar = wCharValues.mapToLong(Long::longValue).average().orElseThrow();
            int sequentialWriteLabel = writePercentiles.percentileNumber(avgWChar);

            labels.put(abstractTaskName, new Labels(cpuLabel, memoryLabel, sequentialReadLabel, sequentialWriteLabel));
        }
    }

    private static class Percentiles {
        private final double minValue;
        private final double range;
        private final int segments;
        private final float[] weights;

        public Percentiles(double minValue, double maxValue, float[] weights) {
            this.minValue = minValue;
            this.range = maxValue - minValue;
            this.segments = weights.length;
            this.weights = weights;
        }

        /**
         * Returns the percentile number for a given value.
         *
         * @param value the value to calculate the percentile number for
         * @return the percentile number (between 1 and segments inclusive)
         */
        public int percentileNumber(double value) {
            float accumulatedWeight = 0;
            if (value < minValue) {
                log.warn("Unexpected value: {} is below the minimum value {}.", value, minValue);
                return 1;
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

    private static long longMin(List<Long> list) {
        return longStream(list).min().orElseThrow();
    }

    private static long longMax(List<Long> list) {
        return longStream(list).max().orElseThrow();
    }

    private static double floatMax(List<Float> list) {
        return doubleStream(list).max().orElseThrow();
    }

    private static double floatMin(List<Float> list) {
        return doubleStream(list).min().orElseThrow();
    }

    private static LongStream longStream(List<Long> list) {
        return list.stream().mapToLong(Long::longValue);
    }

    private static DoubleStream doubleStream(List<Float> list) {
        return list.stream().mapToDouble(Float::doubleValue);
    }
}
