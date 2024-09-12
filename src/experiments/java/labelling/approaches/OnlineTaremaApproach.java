package labelling.approaches;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.LongField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.ConstantEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.NodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.PythonNodeEstimator;
import cws.k8s.scheduler.scheduler.online_tarema.node_estimator.TaskSpecificNodeEstimator;
import labelling.LotaruTraces;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;

/*
 * This class represents the Online Tarema approach to labelling tasks.
 */
@Slf4j
public class OnlineTaremaApproach<T extends Number & Comparable<T>> implements Approach {
    private int nextTaskId = 0;
    private final TraceStorage traceStorage = new TraceStorage();
    private final NodeLabeller nodeLabeller;
    private Map<String, Integer> taskLabels = new HashMap<>();

    @Getter
    String name;
    String shortName;
    private final TraceField<T> target;
    private final Function<String, Float> nodeWeight;
    private final SilhouetteScore<DoublePoint> silhouetteScore;

    static String naiveEstimatorPath = "external/naive_node_estimator.py";
    static String smartEstimatorPath = "external/transitive_node_estimator.py";


    public OnlineTaremaApproach(TraceField<T> target,
                                Function<String, Float> nodeWeight,
                                boolean higherIsBetter,
                                double singlePointClusterScore,
                                NodeEstimator estimator,
                                String name) {
        this.name = String.format("%s(%f)", name, singlePointClusterScore);

        int secondUppercaseLetter = IntStream.range(1, name.length())
                .boxed()
                .filter(i -> Character.isUpperCase(name.charAt(i)))
                .findFirst().orElse(name.length());
        this.shortName = name.substring(0, secondUppercaseLetter).toLowerCase();

        this.target = target;
        this.nodeWeight = nodeWeight;
        this.silhouetteScore = new SilhouetteScore<>(singlePointClusterScore);

        this.nodeLabeller = new NodeLabeller(estimator, higherIsBetter, singlePointClusterScore);
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> naive(TraceField<S> target,
                                                                                   Function<String, Float> nodeWeight,
                                                                                   boolean higherIsBetter,
                                                                                   double singlePointClusterScore,
                                                                                   Set<String> nodes) {
        NodeEstimator<S> naiveEstimator = new PythonNodeEstimator<>(naiveEstimatorPath, nodes);
        return new OnlineTaremaApproach<>(
                target, nodeWeight, higherIsBetter, singlePointClusterScore,
                naiveEstimator, "NaiveOnlineTarema"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> transitive(TraceField<S> target,
                                                                                        Function<String, Float> nodeWeight,
                                                                                        boolean higherIsBetter,
                                                                                        double singlePointClusterScore,
                                                                                        Set<String> nodes) {
        NodeEstimator<S> transitiveEstimator = new PythonNodeEstimator<>(smartEstimatorPath, nodes);
        return new OnlineTaremaApproach<>(
                target, nodeWeight, higherIsBetter, singlePointClusterScore,
                transitiveEstimator, "TransitiveOnlineTarema"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> java(TraceField<S> target,
                                                                                        Function<String, Float> nodeWeight,
                                                                                        boolean higherIsBetter,
                                                                                        double singlePointClusterScore,
                                                                                        Set<String> nodes) {
        NodeEstimator<S> transitiveEstimator = new TaskSpecificNodeEstimator<>(nodes, 30L);
        return new OnlineTaremaApproach<>(
                target, nodeWeight, higherIsBetter, singlePointClusterScore,
                transitiveEstimator, "JavaOnlineTarema"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> tarema(TraceField<S> target,
                                                                                    Function<String, Float> nodeWeight,
                                                                                    Map<String, Double> estimations,
                                                                                    boolean higherIsBetter,
                                                                                    double singlePointClusterScore) {
        NodeEstimator<S> constantEstimator = new ConstantEstimator<>(estimations);
        return new OnlineTaremaApproach<>(
                target, nodeWeight, higherIsBetter, singlePointClusterScore,
                constantEstimator, "SimplifiedTarema"
        );
    }

    @Override
    public void onTaskTermination(TraceRecord trace, TaskConfig config, String nodeName) {
        Optional<Integer> optionalId = traceStorage.saveTrace(trace, nextTaskId, config, nodeName);
        if (optionalId.isEmpty()) {
            return;
        }
        int id = optionalId.get();
        nextTaskId++;

        long charactersRead = traceStorage.getForId(id, LongField.CHARACTERS_READ);
        T realtime = traceStorage.getForId(id, target);
        nodeLabeller.addDataPoint(nodeName, config.getTask(), charactersRead, realtime);
    }

    @Override
    public void recalculate() {
        nodeLabeller.updateLabels();

        if (!nodeLabeller.getLabels().isEmpty()) {
            float[] groupWeights = GroupWeights.forLabels(nodeLabeller.getLabels(), nodeWeight);
            taskLabels = TaskLabeller.logarithmicTaskLabels(traceStorage, target, groupWeights);
        }
    }

    @Override
    public void printNodeLabels() {
        List<Cluster<DoublePoint>> clusters = new ArrayList<>();
        if (nodeLabeller.getMaxLabel() == null) {
            System.out.println("NOT READY");
            return;
        }
        for (int i = 0; i <= nodeLabeller.getMaxLabel(); i++) {
            clusters.add(new Cluster<>());
        }
        for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
            Integer label = (Integer) nodeLabeller.getLabels().get(nodeName);
            if (label == null) {
                System.out.printf("%s(nan) ", nodeName);
                continue;
            }
            System.out.printf("%s(%d) ", nodeName, label);
            double value = LotaruTraces.cpuBenchmarks.get(nodeName);
            DoublePoint point = new DoublePoint(new double[]{value});
            clusters.get(label).addPoint(point);
        }
        double score = silhouetteScore.score(clusters);
        System.out.printf(" -> Silhouette score: %f\n", score);
        for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
            Double estimation = (Double) nodeLabeller.getEstimations().get(nodeName);
            if (estimation == null) {
                System.out.printf("%s(nan) ", nodeName);
            } else {
                System.out.printf("%s(%f) ", nodeName, estimation);
            }
        }
        System.out.println();
    }

    @Override
    public void printTaskLabels() {
        System.out.println(taskLabels);
        for (String task : traceStorage.getAbstractTaskNames()) {
            System.out.printf("%s(%d) ", task, taskLabels.get(task));
        }
        System.out.println();
    }

    @Override
    public void writeState(String experimentDir, List<String> taskNames) {
        File dir = new File(String.format("%s/%s", experimentDir, shortName));
        if (!dir.exists()) {
            boolean success = dir.mkdirs();
            if (!success) {
                log.error("Could not create directory {}", dir);
                System.exit(1);
            }
        }

        boolean newlyCreated;
        File nodeLabelFile = new File(String.format("%s/%s/node_labels.csv", experimentDir, shortName));
        try {
            newlyCreated = nodeLabelFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(nodeLabelFile, true))) {
            if (newlyCreated) {
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s,", nodeName);
                }
                writer.println();
            }
            for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                Integer label = (Integer) nodeLabeller.getLabels().get(nodeName);
                if (label == null) {
                    writer.printf("nan,");
                } else {
                    writer.printf("%d,", label);
                }
            }
            writer.println();
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", nodeLabelFile);
            System.exit(1);
        }

        File nodeEstimationFile = new File(String.format("%s/%s/node_estimations.csv", experimentDir, shortName));
        try {
            newlyCreated = nodeEstimationFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(nodeEstimationFile, true))) {
            if (newlyCreated) {
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    writer.printf("%s,", nodeName);
                }
                writer.println();
            }
            for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                Double estimation = (Double) nodeLabeller.getEstimations().get(nodeName);
                if (estimation == null) {
                    writer.printf("nan,");
                } else {
                    writer.printf("%f,", estimation);
                }
            }
            writer.println();
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", nodeEstimationFile);
            System.exit(1);
        }

        File nodeSilhFile = new File(String.format("%s/%s/node_silh.csv", experimentDir, shortName));
        try {
            newlyCreated = nodeSilhFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(nodeSilhFile, true))) {
            if (newlyCreated) {
                writer.println("silh_score");
            }

            List<Cluster<DoublePoint>> clusters = new ArrayList<>();
            if (nodeLabeller.getMaxLabel() == null) {
                writer.println("nan");
                return;
            } else {
                for (int i = 0; i <= nodeLabeller.getMaxLabel(); i++) {
                    clusters.add(new Cluster<>());
                }
                for (String nodeName : LotaruTraces.getNodesIncludingLocal()) {
                    Integer label = (Integer) nodeLabeller.getLabels().get(nodeName);
                    if (label == null) {
                        continue;
                    }
                    double value = LotaruTraces.cpuBenchmarks.get(nodeName);
                    DoublePoint point = new DoublePoint(new double[]{value});
                    clusters.get(label).addPoint(point);
                }
                double score = silhouetteScore.score(clusters);

                writer.println(score);
            }
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", nodeSilhFile);
            System.exit(1);
        }

        File taskLabelFile = new File(String.format("%s/%s/task_labels.csv", experimentDir, shortName));
        try {
            newlyCreated = taskLabelFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(taskLabelFile, true))) {
            if (newlyCreated) {
                for (String task : taskNames) {
                    writer.printf("%s,", task);
                }
                writer.println();
            }
            for (String task : taskNames) {
                Integer label = taskLabels.get(task);
                if (label == null) {
                    writer.printf("nan,");
                } else {
                    writer.printf("%d,", label);
                }
            }
            writer.println();
        } catch (FileNotFoundException e) {
            log.error("File {} does not exist", taskLabelFile);
            System.exit(1);
        }
    }
}
