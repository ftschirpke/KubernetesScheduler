package labelling;

import cws.k8s.scheduler.scheduler.online_tarema.PointWithName;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import org.apache.commons.math3.ml.clustering.Cluster;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class BestClusterings {

    static List<List<String>> copy(List<List<String>> clustering) {
        List<List<String>> copy = new ArrayList<>();
        for (List<String> cluster : clustering) {
            copy.add(new ArrayList<>(cluster));
        }
        return copy;
    }

    static List<List<List<String>>> produceAllPossibleClusterings() {
        List<List<List<String>>> clusterings = new ArrayList<>();
        for (int k = 2; k < LotaruTraces.cpuBenchmarks.size(); k++) {
            Queue<List<List<String>>> queue = new ArrayDeque<>();
            List<List<String>> empty = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                empty.add(new ArrayList<>());
            }
            queue.add(empty);

            for (String node : LotaruTraces.nodes) {
                int todoCount = queue.size();
                for (int i = 0; i < todoCount; i++) {
                    List<List<String>> clustering = queue.poll();
                    assert clustering != null;
                    for (int j = 0; j < k; j++) {
                        List<List<String>> newClustering = copy(clustering);
                        newClustering.get(j).add(node);
                        queue.add(newClustering);
                    }
                }
            }
            for (List<List<String>> clustering : queue) {
                if (clustering.stream().noneMatch(List::isEmpty)) {
                    clusterings.add(clustering);
                }
            }
        }
        return clusterings;
    }

    static List<List<List<String>>> myClusterings() {
        List<List<String>> clustering5a = List.of(
                List.of("a1", "a2"), List.of("n1"), List.of("local"), List.of("n2"), List.of("c2")
        );
        List<List<String>> clustering5b = List.of(
                List.of("a1", "a2"), List.of("n1"), List.of("local", "n2"), List.of("c2")
        );
        List<List<String>> clustering4a = List.of(
                List.of("a1", "a2"), List.of("n1"), List.of("local", "n2"), List.of("c2")
        );
        List<List<String>> clustering4b = List.of(
                List.of("a1", "a2"), List.of("n1"), List.of("local"), List.of("n2", "c2")
        );
        List<List<String>> clustering3a = List.of(
                List.of("a1", "a2", "n1"), List.of("local", "n2"), List.of("c2")
        );
        List<List<String>> clustering3b = List.of(
                List.of("a1", "a2"), List.of("n1", "local"), List.of("n2", "c2")
        );
        List<List<String>> clustering3c = List.of(
                List.of("a1", "a2"), List.of("n1", "local", "n2"), List.of("c2")
        );
        List<List<String>> clustering2a = List.of(
                List.of("a1", "a2"), List.of("n1", "local", "n2", "c2")
        );
        List<List<String>> clustering2b = List.of(
                List.of("a1", "a2", "n1"), List.of("local", "n2", "c2")
        );

        return List.of(clustering5a, clustering5b,
                clustering4a, clustering4b,
                clustering3a, clustering3b, clustering3c,
                clustering2a, clustering2b);
    }

    public static void main(String[] args) {

        double[] scores = {0, 0.5, 0.66, 0.8, 0.9};
        List<List<List<String>>> clusterings = produceAllPossibleClusterings();
        assert !clusterings.isEmpty();
        for (List<List<String>> clustering : clusterings) {
            for (List<String> cluster : clustering) {
                System.out.print(cluster + " ");
            }
            System.out.println();
        }
        System.out.println("=============================================");
        for (double spc : scores) {
            double bestScore = 0;
            List<Cluster<PointWithName<String>>> bestClustering = null;
            for (List<List<String>> clustering : clusterings) {
                final List<Cluster<PointWithName<String>>> clusters = new ArrayList<>();
                for (List<String> nodesInCluster : clustering) {
                    Cluster<PointWithName<String>> c = new Cluster<>();
                    for (String node : nodesInCluster) {
                        c.addPoint(new PointWithName<>(node, LotaruTraces.cpuBenchmarks.get(node)));
                    }
                    clusters.add(c);
                }
                SilhouetteScore<PointWithName<String>> silhouetteScore = new SilhouetteScore<>(spc);
                double score = silhouetteScore.score(clusters);
                if (score > bestScore) {
                    bestScore = score;
                    bestClustering = clusters;
                }
            }
            System.out.println("=============================================");
            System.out.println("Best score for spc=" + spc + ": " + bestScore);
            for (Cluster<PointWithName<String>> cluster : bestClustering) {
                System.out.println(cluster.getPoints().stream().map(PointWithName::getName).toList());
            }
        }


    }
}
