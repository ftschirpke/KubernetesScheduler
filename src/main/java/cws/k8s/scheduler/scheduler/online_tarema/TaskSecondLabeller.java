package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Getter
@Slf4j
public class TaskSecondLabeller {

    private Map<String, Labels> labels = new HashMap<>();

    /**
     * Recalculates the labels for all tasks based on the historic traces and weights for each label group.
     */
    public void recalculateLabels(NextflowTraceStorage traces, GroupWeights groupWeights) {
        if (traces.empty()) {
            log.info("No traces to calculate node labels from");
            return;
        }
        List<Float> allCpuPercentages = traces.getAll(FloatField.CPU_PERCENTAGE);
        Percentiles cpuPercentiles = Percentiles.fromFloatValues(allCpuPercentages, groupWeights.cpu());
        List<Long> allRssValues = traces.getAll(LongField.RESIDENT_SET_SIZE);
        Percentiles memoryPercentiles = Percentiles.fromLongValues(allRssValues, groupWeights.ram());
        List<Long> allRCharValues = traces.getAll(LongField.CHARACTERS_READ);
        Percentiles readPercentiles = Percentiles.fromLongValues(allRCharValues, groupWeights.read());
        List<Long> allWCharValues = traces.getAll(LongField.CHARACTERS_WRITTEN);
        Percentiles writePercentiles = Percentiles.fromLongValues(allWCharValues, groupWeights.write());

        Map<String, Labels> newLabels = new HashMap<>();
        for (String abstractTaskName : traces.getAbstractTaskNames()) {
            Stream<Float> cpuValues = traces.getForAbstractTask(abstractTaskName, FloatField.CPU_PERCENTAGE);
            double avgCpuPercentage = cpuValues.mapToDouble(Float::doubleValue).average().orElseThrow();
            int cpuLabel = cpuPercentiles.percentileNumber(avgCpuPercentage);

            Stream<Long> rssValues = traces.getForAbstractTask(abstractTaskName, LongField.RESIDENT_SET_SIZE);
            double avgRss = rssValues.mapToLong(Long::longValue).average().orElseThrow();
            int memoryLabel = memoryPercentiles.percentileNumber(avgRss);

            Stream<Long> rCharValues = traces.getForAbstractTask(abstractTaskName, LongField.CHARACTERS_READ);
            double avgRChar = rCharValues.mapToLong(Long::longValue).average().orElseThrow();
            int sequentialReadLabel = readPercentiles.percentileNumber(avgRChar);

            Stream<Long> wCharValues = traces.getForAbstractTask(abstractTaskName, LongField.CHARACTERS_WRITTEN);
            double avgWChar = wCharValues.mapToLong(Long::longValue).average().orElseThrow();
            int sequentialWriteLabel = writePercentiles.percentileNumber(avgWChar);

            newLabels.put(abstractTaskName, new Labels(cpuLabel, memoryLabel, sequentialReadLabel, sequentialWriteLabel));
        }
        labels = newLabels;
    }
}
