package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.nextflow_trace.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskLabeller {
    public static <T extends Number & Comparable<T>> Map<String, Integer> taskLabels(TraceStorage traces,
                                                                                     TraceField<T> field,
                                                                                     float[] groupWeights) {
        if (traces.empty()) {
            return Map.of();
        }
        List<T> allValues = traces.getAll(field);
        Percentiles percentiles = Percentiles.from(allValues, groupWeights);
        return traces.getAbstractTaskNames().stream()
                .map(abstractTaskName -> {
                    Stream<T> taskValues = traces.getForAbstractTask(abstractTaskName, field);
                    double avgValue = taskValues.mapToDouble(T::doubleValue).average().orElseThrow();
                    int label = percentiles.percentileNumber(avgValue);
                    return Map.entry(abstractTaskName, label);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
