package cws.k8s.scheduler.scheduler.online_tarema;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.evaluation.ClusterEvaluator;

import java.util.List;

public class SilhouetteScore<T extends Clusterable> extends ClusterEvaluator<T> {

    public static final double DEFAULT_ONE_POINT_CLUSTER_SCORE = 0.0;

    private final double onePointClusterScore;

    public SilhouetteScore() {
        this(DEFAULT_ONE_POINT_CLUSTER_SCORE);
    }

    public SilhouetteScore(double onePointClusterScore) {
        super();
        this.onePointClusterScore = onePointClusterScore;
    }


    private double scoreSinglePoint(final List<? extends Cluster<T>> allClusters,
                                    final Cluster<T> cluster,
                                    final T point) {
        List<T> clusterPoints = cluster.getPoints();
        if (clusterPoints.size() == 1) {
            return onePointClusterScore;
        }
        double avgIntraClusterDist = clusterPoints.stream()
                .filter(otherPoint -> otherPoint != point)
                .mapToDouble(otherPoint -> distance(point, otherPoint))
                .average().orElse(0.0);
        double nearestOtherClusterDistance = allClusters.stream()
                .filter(otherCluster -> otherCluster != cluster)
                .mapToDouble(otherCluster ->
                        otherCluster.getPoints().stream()
                                .mapToDouble(otherPoint -> distance(point, otherPoint))
                                .sum()
                )
                .min().orElse(Double.MAX_VALUE);

        double denominator = Math.max(nearestOtherClusterDistance, avgIntraClusterDist);
        if (denominator == 0) {
            return -1.0;
        }
        return (nearestOtherClusterDistance - avgIntraClusterDist) / denominator;
    }

    @Override
    public double score(final List<? extends Cluster<T>> clusters) {
        return clusters.stream().flatMapToDouble(cluster ->
                cluster.getPoints().stream().mapToDouble(point -> scoreSinglePoint(clusters, cluster, point))
        ).average().orElse(0);
    }

    @Override
    public boolean isBetterScore(final double score1, final double score2) {
        return score1 > score2;
    }

    public List<CentroidCluster<T>> findBestKmeansClustering(List<T> points) {
        if (points.isEmpty()) {
            return List.of();
        } else if (points.size() == 1) {
            return List.of(new CentroidCluster<>(points.get(0)));
        }
        List<CentroidCluster<T>> bestClustering = null;
        double bestScore = -1;
        for (int k = 2; k < points.size(); k++) {
            KMeansPlusPlusClusterer<T> clusterer = new KMeansPlusPlusClusterer<>(k, 1000);
            List<CentroidCluster<T>> clusters = clusterer.cluster(points);
            double score = score(clusters);
            if (bestClustering == null | isBetterScore(score, bestScore)) {
                bestClustering = clusters;
                bestScore = score;
            }
        }
        if (bestScore <= 0) { // TODO: think about if this threshold is good
            CentroidCluster<T> singleClusterForAll = new CentroidCluster<>(points.get(0));
            for (T point : points) {
                singleClusterForAll.addPoint(point);
            }
            return List.of(singleClusterForAll);
        }
        return bestClustering;
    }

}
