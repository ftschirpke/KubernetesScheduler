package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.scheduler.trace.FloatField;
import cws.k8s.scheduler.scheduler.trace.LongField;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.CentroidCluster;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Getter
@Slf4j
public class TaskFirstLabeller {
    private Labels maxLabels = null;
    private final Map<String, Labels> labels = new HashMap<>();

    private final SilhouetteScore<LabelledPoint<String>> silhouetteScore = new SilhouetteScore<>();

    public boolean recalculateLabels(NextflowTraceStorage traces) {
        List<LabelledPoint<String>> cpuPoints = traces.getAbstractTaskNames().stream().map(
                taskName -> new LabelledPoint<>(taskName,
                        floatAverage(traces.getForAbstractTask(taskName, FloatField.CPU_PERCENTAGE))
                        // TODO: maybe add more fields e.g. cpus
                )
        ).toList();
        List<LabelledPoint<String>> memPoints = traces.getAbstractTaskNames().stream().map(
                taskName -> new LabelledPoint<>(taskName,
                        longAverage(traces.getForAbstractTask(taskName, LongField.RESIDENT_SET_SIZE))
                        // TODO: maybe add more fields e.g. vmem, peak_rss
                )
        ).toList();
        List<LabelledPoint<String>> readPoints = traces.getAbstractTaskNames().stream().map(
                taskName -> new LabelledPoint<>(taskName,
                        longAverage(traces.getForAbstractTask(taskName, LongField.CHARACTERS_READ))
                )
        ).toList();
        List<LabelledPoint<String>> writePoints = traces.getAbstractTaskNames().stream().map(
                taskName -> new LabelledPoint<>(taskName,
                        longAverage(traces.getForAbstractTask(taskName, LongField.CHARACTERS_WRITTEN))
                )
        ).toList();

        Map<String, Integer> cpuLabels = labelsForPoints(cpuPoints);
        Map<String, Integer> memLabels = labelsForPoints(memPoints);
        Map<String, Integer> readLabels = labelsForPoints(readPoints);
        Map<String, Integer> writeLabels = labelsForPoints(writePoints);

        int maxCpuLabel = cpuLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxMemLabel = memLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxReadLabel = readLabels.values().stream().max(Integer::compareTo).orElse(0);
        int maxWriteLabel = writeLabels.values().stream().max(Integer::compareTo).orElse(0);
        Labels newMaxLabels = new Labels(maxCpuLabel, maxMemLabel, maxReadLabel, maxWriteLabel);

        Map<String, Labels> newLabels = traces.getAbstractTaskNames().stream()
                .map(taskName -> Map.entry(taskName, new Labels(
                        cpuLabels.get(taskName),
                        memLabels.get(taskName),
                        readLabels.get(taskName),
                        writeLabels.get(taskName)
                ))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        boolean changed;
        synchronized (labels) {
            changed = !newLabels.equals(labels);
            if (!newLabels.keySet().containsAll(labels.keySet())) {
                log.error("Online Tarema Scheduler: New labels do not contain all nodes; lost nodes");
                labels.clear();
            }
            labels.putAll(newLabels);
            maxLabels = newMaxLabels;
        }
        log.info("DEBUG: recalculateLabelsFromSpeedEstimations 4");
        return changed;
    }

    private Map<String, Integer> labelsForPoints(List<LabelledPoint<String>> points) {
        List<CentroidCluster<LabelledPoint<String>>> clusters = silhouetteScore.findBestKmeansClustering(points);
        if (clusters.size() == 1) {
            return points.stream()
                    .map(labelledPoint -> Map.entry(labelledPoint.getLabel(), 0))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // TODO: careful, this sorting approach only works for one-dimensional points
        clusters.sort(Comparator.comparingDouble(cluster -> cluster.getCenter().getPoint()[0]));

        return IntStream.range(0, clusters.size())
                .boxed()
                .flatMap(i -> clusters.get(i).getPoints().stream()
                        .map(point -> Map.entry(point.getLabel(), i))
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static double floatAverage(Stream<Float> stream) {
        return stream.mapToDouble(Float::doubleValue).average().orElseThrow();
    }

    static double longAverage(Stream<Long> stream) {
        return stream.mapToLong(Long::longValue).average().orElseThrow();
    }

}
