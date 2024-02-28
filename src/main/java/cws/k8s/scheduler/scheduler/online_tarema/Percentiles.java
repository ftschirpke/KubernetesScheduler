package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
class Percentiles {
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

    public static Percentiles fromFloatValues(List<Float> values, float[] weights) {
        return new Percentiles(
                values.stream().min(Float::compareTo).orElse(0f),
                values.stream().max(Float::compareTo).orElse(0f),
                weights);
    }

    public static Percentiles fromLongValues(List<Long> values, float[] weights) {
        return new Percentiles(
                values.stream().min(Long::compareTo).orElse(0L),
                values.stream().max(Long::compareTo).orElse(0L),
                weights);
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
