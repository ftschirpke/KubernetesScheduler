package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.NodeWithAlloc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;

public class GroupWeights {
    public static <T> float[] forLabels(Integer maxLabel, Map<T, Integer> labels) {
        return GroupWeights.forLabels(maxLabel, labels, node -> 1.0f);
    }

    public static <T> float[] forLabels(Integer maxLabel,
                                        Map<T, Integer> labels,
                                        Function<T, Float> keyWeight) {
        if (maxLabel == null || labels == null || labels.isEmpty()) {
            return null;
        }

        float totalWeights = 0;
        float[] weightsPerGroup = new float[maxLabel + 1];

        for (Map.Entry<T, Integer> e : labels.entrySet()) {
            T key = e.getKey();
            Integer label = e.getValue();
            Float weight = keyWeight.apply(key);
            totalWeights += weight;
            weightsPerGroup[label] += weight;
        }

        for (int i = 0; i < weightsPerGroup.length; i++) {
            weightsPerGroup[i] /= totalWeights;
        }
        return weightsPerGroup;
    }

    public static Float cpuNodeWeight(NodeWithAlloc node) {
        return node.getMaxResources().getCpu().floatValue();
    }

    public static Float memoryNodeWeight(NodeWithAlloc node) {
        return node.getMaxResources().getRam()
                .divide(BigDecimal.valueOf(10_000_000L), RoundingMode.HALF_UP)
                .floatValue();
    }
}
