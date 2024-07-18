package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import cws.k8s.scheduler.util.Tuple;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class TaskSpecificNodeEstimator<T extends Number> implements NodeEstimator<T> {
    private int estimationsCounter = 0;
    private Set<String> nodeNames = new HashSet<>();

    record DataPoint<T extends Number>(String node, String task, long rchar, T target) {
    }

    static private final int initialDataCapacity = 200;
    static private final int initialNodeCapacity = 10;
    static private final int initialTaskCapacity = 30;

    private final List<DataPoint<T>> data = new ArrayList<>(initialDataCapacity);
    private final List<String> nodes = new ArrayList<>(initialNodeCapacity);
    private final List<String> tasks = new ArrayList<>(initialTaskCapacity);
    private final Map<String, Map<String, List<DataPoint<T>>>> unprocessedSamples = new HashMap<>(initialTaskCapacity);
    private final Map<String, Map<String, List<DataPoint<T>>>> readySampleGroups = new HashMap<>(initialTaskCapacity);
    private final Set<Tuple<String, String>> linesToUpdate = new HashSet<>();
    private final Map<String, Map<String, Line>> lines = new HashMap<>(initialTaskCapacity);
    private final Map<String, Map<String, Range>> ranges = new HashMap<>(initialTaskCapacity);
    private final Map<String, Map<String, Integer>> dataCounts = new HashMap<>(initialTaskCapacity);
    private final Map<String, QuadraticMatrix<Double>> ratioMatricesByTask = new HashMap<>(initialTaskCapacity);
    private final Map<String, QuadraticMatrix<Integer>> weightMatricesByTask = new HashMap<>(initialTaskCapacity);
    private QuadraticMatrix<Boolean> comparisonPossible = null;

    public TaskSpecificNodeEstimator(Set<String> nodeNames) {
        this.nodeNames = nodeNames;
    }

    private void addNode(String node) {
        nodes.add(node);
        for (QuadraticMatrix<Double> matrix : ratioMatricesByTask.values()) {
            matrix.addDimensionWithValue(0.0);
        }
        for (QuadraticMatrix<Integer> matrix : weightMatricesByTask.values()) {
            matrix.addDimensionWithValue(0);
        }
        if (comparisonPossible == null) {
            comparisonPossible = new QuadraticMatrix<>(1, false);
        } else {
            comparisonPossible.addDimensionWithValue(false);
        }
    }

    private void addTask(String task) {
        tasks.add(task);
        int nodeCount = nodes.size();
        ratioMatricesByTask.put(task, new QuadraticMatrix<>(nodeCount, 0.0));
        weightMatricesByTask.put(task, new QuadraticMatrix<>(nodeCount, 0));
    }

    public synchronized void addDataPoint(String nodeName, String taskName, long rchar, T targetValue) {
        if (!nodeNames.contains(nodeName)) {
            log.error("Ignore attempt to add data point for unknown node: {}", nodeName);
            return;
        }
        DataPoint<T> sample = new DataPoint<>(nodeName, taskName, rchar, targetValue);
        if (dataCounts.get(taskName).get(nodeName) > 0) {
            addSample(sample);
        } else if (unprocessedSamples.get(taskName).containsKey(nodeName)) {
            Set<T> uniqueTargetValues = unprocessedSamples.get(taskName).get(nodeName)
                    .stream()
                    .map(dataPoint -> dataPoint.target)
                    .collect(Collectors.toSet());
            uniqueTargetValues.add(targetValue);
            if (uniqueTargetValues.size() == 1) {
                unprocessedSamples.get(taskName).get(nodeName).add(sample);
                return;
            }
            List<DataPoint<T>> olderSamples = unprocessedSamples.get(taskName).remove(nodeName);
            if (tasks.contains(taskName)) {
                for (DataPoint<T> olderSample : olderSamples) {
                    addSample(olderSample);
                }
                addSample(sample);
            }
        }
    }

    private void addSample(DataPoint<T> sample) {
        data.add(sample);
        if (!nodes.contains(sample.node)) {
            addNode(sample.node);
        }
        if (!tasks.contains(sample.task)) {
            addTask(sample.task);
        }
        dataCounts.get(sample.task).merge(sample.node, 1, Integer::sum);
        linesToUpdate.add(new Tuple<>(sample.task, sample.node));
    }

    private void updateLines() {
        int size = nodes.size();
        for (Tuple<String, String> toUpdate : linesToUpdate) {
            String task = toUpdate.getA();
            String node = toUpdate.getB();
            boolean lineChanged = updateLine(task, node);
            if (lineChanged) {
                updateRatios(task, node);
            }
        }
        linesToUpdate.clear();
    }

    public QuadraticMatrix<Double> accumulatedRatios() {
        // TODO
    }

    private boolean updateLine(String task, String node) {
        // TODO
    }

    private void updateRatios(String task, String node) {
        // TODO
    }

    private Tuple<QuadraticMatrix<Double>, QuadraticMatrix<Double>> floydWarshall() {
        // TODO
    }

    public QuadraticMatrix<Double> transitiveRatios() {
        // TODO
    }

    public NodeRankings ranking() {
        // TODO
    }

    @Override
    public Map<String, Double> estimations() {
        return ranking().generalRanking;
    }

    public static record NodeRankings(
            Map<String, Double> generalRanking,
            Map<String, Map<String, Double>> taskSpecificRankings
    ) {}

    public static class QuadraticMatrix<D> {
        @Getter
        private int dimension;
        private final List<List<D>> data;

        QuadraticMatrix(int size, D value) {
            this.data = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                List<D> new_row = new ArrayList<>(Collections.nCopies(dimension, value));
                this.data.add(new_row);
            }
        }

        D get(int i, int j) {
            return data.get(i).get(j);
        }

        void set(int i, int j, D val) {
            data.get(i).set(j, val);
        }

        void addDimensionWithValue(D value) {
            if (data.size() != dimension || data.stream().anyMatch(l -> l.size() != dimension)) {
                throw new IllegalStateException("Dimension should always match up with list sizes.");
            }
            for (List<D> inner : data) {
                inner.add(value);
            }
            List<D> new_row = new ArrayList<>(Collections.nCopies(dimension, value));
            data.add(new_row);
        }
    }

    private static record Range(double start, double end) {
        double width() {
            return end - start;
        }

        Range intersection(Range other) {
            double new_start = Double.max(start, other.start);
            double new_end = Double.max(end, other.end);
            return new Range(new_start, new_end);
        }
    }

    private static record Line(double coef, double intercept) {
        double evaluate(double x) {
            return coef * x + intercept;
        }

        double avg_on_interval(Range interval) {
            double first = evaluate(interval.start);
            double second = evaluate(interval.end);
            return (first + second) / 2;
        }

        /*
         * Compares two lines on the given interval.
         * Returns the fraction of the average of the self line to the average of the other line.
         */
        Double compare_on_interval(Line other, Range interval) {
            double thisAvg = this.avg_on_interval(interval);
            double otherAvg = other.avg_on_interval(interval);
            if (thisAvg == 0 || otherAvg == 0) {
                return null;
            }
            return thisAvg / otherAvg;
        }
    }
}
