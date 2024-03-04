package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.IntegerField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskLabeller {
    public static Map<String, Integer> taskLabels(NextflowTraceStorage traces, FloatField field, float[] groupWeights) {
        if (traces.empty()) {
            return Map.of();
        }
        List<Float> allValues = traces.getAll(field);
        Percentiles percentiles = Percentiles.fromFloatValues(allValues, groupWeights);
        return traces.getAbstractTaskNames().stream()
                .map(abstractTaskName -> {
                    Stream<Float> taskValues = traces.getForAbstractTask(abstractTaskName, field);
                    double avgValue = taskValues.mapToDouble(Float::doubleValue).average().orElseThrow();
                    int label = percentiles.percentileNumber(avgValue);
                    return Map.entry(abstractTaskName, label);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Integer> taskLabels(NextflowTraceStorage traces, LongField field, float[] groupWeights) {
        if (traces.empty()) {
            return Map.of();
        }
        List<Long> allValues = traces.getAll(field);
        Percentiles percentiles = Percentiles.fromLongValues(allValues, groupWeights);
        return traces.getAbstractTaskNames().stream()
                .map(abstractTaskName -> {
                    Stream<Long> taskValues = traces.getForAbstractTask(abstractTaskName, field);
                    double avgValue = taskValues.mapToLong(Long::longValue).average().orElseThrow();
                    int label = percentiles.percentileNumber(avgValue);
                    return Map.entry(abstractTaskName, label);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    public static Map<String, Integer> taskLabels(NextflowTraceStorage traces, IntegerField field, float[] groupWeights) {
        if (traces.empty()) {
            return Map.of();
        }
        List<Integer> allValues = traces.getAll(field);
        Percentiles percentiles = Percentiles.fromIntegerValues(allValues, groupWeights);
        return traces.getAbstractTaskNames().stream()
                .map(abstractTaskName -> {
                    Stream<Integer> taskValues = traces.getForAbstractTask(abstractTaskName, field);
                    double avgValue = taskValues.mapToLong(Integer::longValue).average().orElseThrow();
                    int label = percentiles.percentileNumber(avgValue);
                    return Map.entry(abstractTaskName, label);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
