package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
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
import labelling.LotaruTraces;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;

import java.io.*;
import java.util.*;
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
    private final SilhouetteScore<DoublePoint> silhouetteScore;

    static String naiveEstimatorPath = "external/naive_node_estimator.py";
    static String smartEstimatorPath = "external/transitive_node_estimator.py";


    public OnlineTaremaApproach(TraceField<T> target,
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
        this.silhouetteScore = new SilhouetteScore<>(singlePointClusterScore);

        this.nodeLabeller = new NodeLabeller(estimator, higherIsBetter, singlePointClusterScore);
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> naive(TraceField<S> target,
                                                                                   boolean higherIsBetter,
                                                                                   double singlePointClusterScore) {
        NodeEstimator naiveEstimator = new PythonNodeEstimator(naiveEstimatorPath, Set.of(LotaruTraces.nodes));
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, singlePointClusterScore,
                naiveEstimator, "NaiveOnlineTarema"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> transitive(TraceField<S> target,
                                                                                        boolean higherIsBetter,
                                                                                        double singlePointClusterScore) {
        NodeEstimator transitiveEstimator = new PythonNodeEstimator(smartEstimatorPath, Set.of(LotaruTraces.nodes));
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, singlePointClusterScore,
                transitiveEstimator, "TransitiveOnlineTarema"
        );
    }

    public static <S extends Number & Comparable<S>> OnlineTaremaApproach<S> tarema(TraceField<S> target,
                                                                                    Map<NodeWithAlloc, Double> estimations,
                                                                                    boolean higherIsBetter,
                                                                                    double singlePointClusterScore) {
        NodeEstimator constantEstimator = new ConstantEstimator(estimations);
        return new OnlineTaremaApproach<>(
                target, higherIsBetter, singlePointClusterScore,
                constantEstimator, "SimplifiedTarema"
        );
    }

    @Override
    public void onTaskTermination(TraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        int id = traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        long charactersRead = traceStorage.getForId(id, LongField.CHARACTERS_READ);
        long realtime = traceStorage.getForId(id, LongField.REALTIME);
        nodeLabeller.addDataPoint(node, config.getTask(), charactersRead, realtime);
    }

    @Override
    public void recalculate() {
        nodeLabeller.updateLabels();

        if (!nodeLabeller.getLabels().isEmpty()) {
            float[] groupWeights = GroupWeights.forLabels(nodeLabeller.getMaxLabel(), nodeLabeller.getLabels());
            // HACK: to change the target field, change the Field here
            taskLabels = TaskLabeller.logarithmicTaskLabels(traceStorage, LongField.REALTIME, groupWeights);
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
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            int label = nodeLabeller.getLabels().get(node);
            System.out.printf("%s(%d) ", node.getName(), label);
            double value = LotaruTraces.cpuBenchmarks.get(node);
            DoublePoint point = new DoublePoint(new double[]{value});
            clusters.get(label).addPoint(point);
        }
        double score = silhouetteScore.score(clusters);
        System.out.printf(" -> Silhouette score: %f\n", score);
        for (NodeWithAlloc node : LotaruTraces.nodes) {
            Double estimation = nodeLabeller.getEstimations().get(node);
            System.out.printf("%s(%f) ", node.getName(), estimation);
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
                for (NodeWithAlloc node : LotaruTraces.nodes) {
                    writer.printf("%s,", node.getName());
                }
                writer.println();
            }
            for (NodeWithAlloc node : LotaruTraces.nodes) {
                Integer label = nodeLabeller.getLabels().get(node);
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
                for (NodeWithAlloc node : LotaruTraces.nodes) {
                    writer.printf("%s,", node.getName());
                }
                writer.println();
            }
            for (NodeWithAlloc node : LotaruTraces.nodes) {
                Double estimation = nodeLabeller.getEstimations().get(node);
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
                for (NodeWithAlloc node : LotaruTraces.nodes) {
                    int label = nodeLabeller.getLabels().get(node);
                    double value = LotaruTraces.cpuBenchmarks.get(node);
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
