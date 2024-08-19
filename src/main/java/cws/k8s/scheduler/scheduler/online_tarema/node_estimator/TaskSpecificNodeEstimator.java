package cws.k8s.scheduler.scheduler.online_tarema.node_estimator;

import cws.k8s.scheduler.util.Tuple;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class TaskSpecificNodeEstimator<T extends Number> implements NodeEstimator<T> {
    private final Set<String> nodeNames;
    private final Long taskSpecificThreshold;

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

    public TaskSpecificNodeEstimator(Set<String> nodeNames, Long taskSpecificThreshold) {
        this.nodeNames = nodeNames;
        this.taskSpecificThreshold = taskSpecificThreshold;
    }

    private void addNode(String node) {
        nodes.add(node);
        for (QuadraticMatrix<Double> matrix : ratioMatricesByTask.values()) {
            matrix.addDimensionWithValue(null);
            int last = matrix.dimension - 1;
            matrix.set(last, last, 0.0);
        }
        for (QuadraticMatrix<Integer> matrix : weightMatricesByTask.values()) {
            matrix.addDimensionWithValue(0);
        }
        if (comparisonPossible == null) {
            comparisonPossible = new QuadraticMatrix<>(1, false);
        } else {
            comparisonPossible.addDimensionWithValue(false);
        }
        for (String task : tasks) {
            if (dataCounts.get(task).containsKey(node)) {
                throw new IllegalStateException("Newly added node should not exist yet.");
            }
            dataCounts.get(task).put(node, 0);
        }
    }

    private void addTask(String task) {
        tasks.add(task);
        int nodeCount = nodes.size();
        QuadraticMatrix<Double> taskRatioMatrix = new QuadraticMatrix<>(nodeCount, null);
        for (int i = 0; i < nodeCount; i++) {
            taskRatioMatrix.set(i, i, 0.0);
        }
        ratioMatricesByTask.put(task, taskRatioMatrix);
        weightMatricesByTask.put(task, new QuadraticMatrix<>(nodeCount, 0));
        if (dataCounts.containsKey(task)) {
            throw new IllegalStateException("Newly added task should not exist yet.");
        }
        dataCounts.put(task, new HashMap<>(initialNodeCapacity));
        for (String node : nodes) {
            dataCounts.get(task).put(node, 0);
        }
    }

    public synchronized void addDataPoint(String nodeName, String taskName, long rchar, T targetValue) {
        if (!nodeNames.contains(nodeName)) {
            log.error("Ignore attempt to add data point for unknown node: {}", nodeName);
            return;
        }
        DataPoint<T> sample = new DataPoint<>(nodeName, taskName, rchar, targetValue);
        if (dataCounts.containsKey(taskName) && dataCounts.get(taskName).containsKey(nodeName)
                && dataCounts.get(taskName).get(nodeName) > 0) {
            addSample(sample);
        } else if (unprocessedSamples.containsKey(taskName) && unprocessedSamples.get(taskName).containsKey(nodeName)) {
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
            } else {
                olderSamples.add(sample);
                if (!readySampleGroups.containsKey(taskName)) {
                    readySampleGroups.put(taskName, new HashMap<>(initialNodeCapacity));
                }
                readySampleGroups.get(taskName).put(nodeName, olderSamples);
                if (readySampleGroups.get(taskName).size() > 1) {
                    boolean anyNodeAlreadyKnown = false;
                    for (String node : readySampleGroups.get(taskName).keySet()) {
                        if (nodes.contains(node)) {
                            anyNodeAlreadyKnown = true;
                            break;
                        }
                    }
                    if (anyNodeAlreadyKnown || nodes.isEmpty()) {
                        for (List<DataPoint<T>> sampleGroup : readySampleGroups.remove(taskName).values()) {
                            for (DataPoint<T> sampleToAdd : sampleGroup) {
                                addSample(sampleToAdd);
                            }
                        }
                    }
                }
            }
        } else {
            List<DataPoint<T>> singleItemList = new ArrayList<>();
            singleItemList.add(sample);
            if (!unprocessedSamples.containsKey(taskName)) {
                unprocessedSamples.put(taskName, new HashMap<>(initialNodeCapacity));
            }
            unprocessedSamples.get(taskName).put(nodeName, singleItemList);
            return;
        }
        updateLines();
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

        Set<String> taskSet = new HashSet<>(tasks);
        assert ratioMatricesByTask.keySet().equals(taskSet);
        for (QuadraticMatrix<Double> ratioMatrix : ratioMatricesByTask.values()) {
            assert ratioMatrix.getDimension() == size;
        }
        assert weightMatricesByTask.keySet().equals(taskSet);
        for (QuadraticMatrix<Integer> weightMatrix : weightMatricesByTask.values()) {
            assert weightMatrix.getDimension() == size;
        }

        for (Tuple<String, String> toUpdate : linesToUpdate) {
            String task = toUpdate.getA();
            String node = toUpdate.getB();
            updateLine(task, node);
            updateRatios(task, node);
        }
        linesToUpdate.clear();
    }

    public QuadraticMatrix<Double> accumulatedRatios() {
        int size = nodes.size();
        QuadraticMatrix<Double> weightedRatiosSummed = new QuadraticMatrix<>(size, 0.0);
        QuadraticMatrix<Integer> weightsSummed = new QuadraticMatrix<>(size, 0);
        for (Map.Entry<String, QuadraticMatrix<Double>> entry : ratioMatricesByTask.entrySet()) {
            String task = entry.getKey();
            QuadraticMatrix<Double> ratioMatrix = entry.getValue();
            QuadraticMatrix<Integer> weightMatrix = weightMatricesByTask.get(task);
            assert ratioMatrix.getDimension() == size;
            assert weightMatrix.getDimension() == size;
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    int weight = weightMatrix.get(i, j);
                    Double ratio = ratioMatrix.get(i, j);
                    if (weight == 0 && ratio == null) {
                        continue; // should have weight zero, if ratio is unknown
                    } else if (i == j) {
                        continue; // diagonal should have weight zero
                    } else if (weight == 0 || ratio == null) {
                        throw new IllegalStateException("Weight should be zero iff ratio is unknown (null).");
                    }
                    double previousRatioSum = weightedRatiosSummed.get(i, j);
                    weightedRatiosSummed.set(i, j, previousRatioSum + weight * ratio); // M[i,j] += w * ratio
                    int previousWeightSum = weightsSummed.get(i, j);
                    weightsSummed.set(i, j, previousWeightSum + weight); // W[i,j] += w
                }
            }
        }
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                int weight = weightsSummed.get(i, j);
                if (weight == 0) {
                    if (comparisonPossible.get(i, j)) {
                        throw new IllegalStateException("Weight cannot be zero if comparison is possible.");
                    }
                    continue;
                }
                if (!comparisonPossible.get(i, j)) {
                    throw new IllegalStateException("Weight cannot be non-zero if comparison is not possible.");
                }
                double previousRatioSum = weightedRatiosSummed.get(i, j);
                weightedRatiosSummed.set(i, j, previousRatioSum / (double) weight);
            }
        }
        return weightedRatiosSummed;
    }

    private void updateLine(String task, String node) {
        List<DataPoint<T>> relevantData = data.stream()
                .filter(dataPoint -> dataPoint.task.equals(task) && dataPoint.node.equals(node))
                .toList();

        int dataCount = dataCounts.get(task).get(node);
        assert dataCount == relevantData.size();
        assert dataCount >= 2;

        SimpleRegression model = new SimpleRegression(true);
        for (DataPoint<T> dataPoint : relevantData) {
            model.addData(dataPoint.rchar, dataPoint.target.doubleValue());
        }

        double xMin = relevantData.stream()
                .mapToDouble(dataPoint -> (double) dataPoint.rchar)
                .min().orElseThrow();
        double xMax = relevantData.stream()
                .mapToDouble(dataPoint -> (double) dataPoint.rchar)
                .max().orElseThrow();
        Range newRange = new Range(xMin, xMax);
        if (!ranges.containsKey(task)) {
            ranges.put(task, new HashMap<>(initialNodeCapacity));
        }
        ranges.get(task).put(node, newRange);

        double coef = model.getSlope();
        double intercept = model.getIntercept();
        Line newLine = new Line(coef, intercept);
        if (!lines.containsKey(task)) {
            lines.put(task, new HashMap<>(initialNodeCapacity));
        }
        lines.get(task).put(node, newLine);
    }

    private boolean isInvalidData(int nodeDataCount, Line nodeLine, Range nodeRange) {
        return nodeDataCount < 2 || nodeLine == null || nodeRange == null || nodeRange.width() <= 0;
    }

    private void updateRatios(String task, String node) {
        int i = nodes.indexOf(node);
        int nodeDataCount = dataCounts.get(task).get(node);
        Line nodeLine = lines.get(task).get(node);
        Range nodeRange = ranges.get(task).get(node);
        if (isInvalidData(nodeDataCount, nodeLine, nodeRange)) {
            return;
        }

        QuadraticMatrix<Double> taskRatioMatrix = ratioMatricesByTask.get(task);
        QuadraticMatrix<Integer> taskWeightMatrix = weightMatricesByTask.get(task);
        for (int j = 0; j < nodes.size(); j++) {
            String otherNode = nodes.get(j);
            if (i == j) {
                continue;
            }
            int otherNodeDataCount = dataCounts.get(task).get(otherNode);
            Line otherNodeLine = lines.get(task).get(otherNode);
            Range otherNodeRange = ranges.get(task).get(otherNode);
            if (isInvalidData(otherNodeDataCount, otherNodeLine, otherNodeRange)) {
                continue;
            }
            Range intersectRange = nodeRange.intersection(otherNodeRange);
            if (intersectRange.width() <= 0) {
                continue;
            }
            Double ratio = nodeLine.compareOnInterval(otherNodeLine, intersectRange);
            if (ratio == null || ratio <= 0.0) {
                continue;
            }
            Double logRatio = Math.log(ratio);
            if (logRatio.isInfinite() || logRatio.isNaN()) {
                continue;
            }

            int weight = (nodeDataCount - 1) * (otherNodeDataCount - 1);

            taskRatioMatrix.set(i, j, logRatio);
            taskRatioMatrix.set(j, i, -logRatio);
            taskWeightMatrix.set(i, j, weight);
            taskWeightMatrix.set(j, i, weight);
            comparisonPossible.set(i, j, true);
            comparisonPossible.set(j, i, true);
        }
    }

    private static Tuple<QuadraticMatrix<Integer>, QuadraticMatrix<Integer>> floydWarshall(QuadraticMatrix<Double> matrix) {
        int size = matrix.getDimension();
        QuadraticMatrix<Integer> distances = new QuadraticMatrix<>(size, 1);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (matrix.get(i, j) == null) {
                    distances.set(i, j, size);
                }
            }
        }
        QuadraticMatrix<Integer> predecessors = new QuadraticMatrix<>(size, 0);
        for (int start = 0; start < size; start++) {
            for (int end = 0; end < size; end++) {
                predecessors.set(start, end, start);
            }
        }
        for (int maxNode = 0; maxNode < size; maxNode++) {
            for (int src = 0; src < size; src++) {
                for (int dst = 0; dst < size; dst++) {
                    int srcDist = distances.get(src, maxNode);
                    int dstDist = distances.get(maxNode, dst);
                    if (distances.get(src, dst) > srcDist + dstDist) {
                        distances.set(src, dst, srcDist + dstDist);
                        predecessors.set(src, dst, predecessors.get(maxNode, dst));
                    }
                }
            }
        }
        return new Tuple<>(distances, predecessors);
    }

    private static QuadraticMatrix<Double> transitiveRatios(QuadraticMatrix<Double> ratioMatrix) {
        int size = ratioMatrix.getDimension();
        // create a copy of the input matrix
        QuadraticMatrix<Double> transitiveRatios = new QuadraticMatrix<>(size, 0.0);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                transitiveRatios.set(i, j, ratioMatrix.get(i, j));
            }
        }
        // initialize helper matrices
        Tuple<QuadraticMatrix<Integer>, QuadraticMatrix<Integer>> tup = floydWarshall(ratioMatrix);
        QuadraticMatrix<Integer> distances = tup.getA();
        QuadraticMatrix<Integer> predecessors = tup.getB();
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (distances.get(i, j) >= size) {
                    return null;
                }
            }
        }
        QuadraticMatrix<Boolean> ratiosCalculated = new QuadraticMatrix<>(size, true);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (i == j) {
                    continue;
                }
                if (ratioMatrix.get(i, j) == null) {
                    ratiosCalculated.set(i, j, false); // set null values to "need to be calculated"
                }
            }
        }
        // transitively fill in the copy matrix
        Stack<Integer> todoStack = new Stack<>();
        for (int src = 0; src < size; src++) {
            for (int dst = 0; dst < size; dst++) {
                if (ratiosCalculated.get(src, dst)) {
                    continue;
                }
                while (!ratiosCalculated.get(src, dst)) {
                    todoStack.add(dst);
                    dst = predecessors.get(src, dst);
                }
                Double val = transitiveRatios.get(src, dst);
                while (!todoStack.empty()) {
                    int middle = dst;
                    dst = todoStack.pop();
                    val += transitiveRatios.get(middle, dst);
                    if (val.isNaN() || val.isInfinite()) {
                        // at least one value cannot be properly constructed
                        // TODO: find a better solution than aborting
                        return null;
                    }
                    transitiveRatios.set(src, dst, val);
                    transitiveRatios.set(dst, src, -val);
                    ratiosCalculated.set(src, dst, true);
                    ratiosCalculated.set(dst, src, true);
                }
            }
        }
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                assert ratiosCalculated.get(i, j);
            }
        }
        return transitiveRatios;
    }

    private static List<Double> geometricMeansOfLogarithmicRows(QuadraticMatrix<Double> logMatrix) {
        return logMatrix.data.stream()
                .map(logRow -> logRow.stream().mapToDouble(Double::doubleValue).average().orElseThrow())
                .map(Math::exp)
                .toList();
    }

    private Map<String, Double> nodeEstimationsFromMatrix(QuadraticMatrix<Double> logMatrix) {
        QuadraticMatrix<Double> fullCopy = transitiveRatios(logMatrix);
        if (fullCopy == null) {
            return null;
        }
        List<Double> means = geometricMeansOfLogarithmicRows(fullCopy);
        return IntStream.range(0, nodes.size())
                .mapToObj(i -> new Tuple<>(nodes.get(i), means.get(i)))
                .collect(Collectors.toMap(Tuple::getA, Tuple::getB));
    }

    @Override
    public NodeRankings taskSpecificEstimations() {
        if (nodes.size() < nodeNames.size()) {
            return new NodeRankings(null, null); // we don't have data for all expected nodes
        }
        Map<String, Double> generalRanking = nodeEstimationsFromMatrix(accumulatedRatios());
        if (generalRanking == null) {
            return new NodeRankings(null, null);
        }
        Map<String, Map<String, Double>> taskSpecificRankings = new HashMap<>();

        long threshold = taskSpecificThreshold != null ? taskSpecificThreshold : 3L * nodeNames.size();
        for (String task : tasks) {
            long sampleCount = data.stream().filter(sample -> sample.task.equals(task)).count();
            if (sampleCount < threshold) {
                continue;
            }
            Map<String, Double> taskSpecificRanking = nodeEstimationsFromMatrix(ratioMatricesByTask.get(task));
            if (taskSpecificRanking != null) {
                taskSpecificRankings.put(task, taskSpecificRanking);
            }
        }
        return new NodeRankings(generalRanking, taskSpecificRankings);
    }

    @Override
    public Map<String, Double> estimations() {
        if (nodes.size() < nodeNames.size()) {
            return null; // we don't have data for all expected nodes
        }
        return nodeEstimationsFromMatrix(accumulatedRatios());
    }


    public static class QuadraticMatrix<D> {
        @Getter
        private int dimension;
        private final List<List<D>> data;

        QuadraticMatrix(int size, D value) {
            this.data = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                List<D> new_row = new ArrayList<>(Collections.nCopies(size, value));
                this.data.add(new_row);
            }
            this.dimension = size;
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
            dimension++;
            for (List<D> inner : data) {
                inner.add(value);
                assert inner.size() == dimension;
            }
            List<D> new_row = new ArrayList<>(Collections.nCopies(dimension, value));
            data.add(new_row);
            assert data.size() == dimension;
        }
    }

    private record Range(double start, double end) {
        double width() {
            return end - start;
        }

        Range intersection(Range other) {
            double new_start = Double.max(start, other.start);
            double new_end = Double.max(end, other.end);
            return new Range(new_start, new_end);
        }
    }

    private record Line(double coef, double intercept) {
        double evaluate(double x) {
            return coef * x + intercept;
        }

        double avgOnInterval(Range interval) {
            double first = evaluate(interval.start);
            double second = evaluate(interval.end);
            return (first + second) / 2;
        }

        /*
         * Compares two lines on the given interval.
         * Returns the fraction of the average of the self line to the average of the other line.
         */
        Double compareOnInterval(Line other, Range interval) {
            double thisAvg = this.avgOnInterval(interval);
            double otherAvg = other.avgOnInterval(interval);
            if (thisAvg == 0 || otherAvg == 0) {
                return null;
            }
            return thisAvg / otherAvg;
        }
    }
}
