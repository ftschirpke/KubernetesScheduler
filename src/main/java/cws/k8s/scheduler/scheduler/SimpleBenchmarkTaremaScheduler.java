package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nextflow_trace.FloatField;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceStorage;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.SilhouetteScore;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
/*
 * This class reimplements the original Tarema scheduling but uses only one feature.
 */
public class SimpleBenchmarkTaremaScheduler extends TaremaScheduler {
    private final TraceStorage traces = new TraceStorage();

    private final NodeLabeller.LabelState nodeLabelState;
    private final float[] groupWeights;
    private Map<String, Integer> taskLabels = new HashMap<>();

    public SimpleBenchmarkTaremaScheduler(Map<NodeWithAlloc, Double> speedEstimations,
                                          String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config) {
        this(SilhouetteScore.DEFAULT_ONE_POINT_CLUSTER_SCORE, speedEstimations, execution, client, namespace, config);
    }

    public SimpleBenchmarkTaremaScheduler(double onePointClusterScore,
                                          Map<NodeWithAlloc, Double> speedEstimations,
                                          String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config) {
        super(execution, client, namespace, config);

        nodeLabelState = NodeLabeller.labelOnce(speedEstimations, true, onePointClusterScore);
        groupWeights = GroupWeights.forLabels(nodeLabelState.maxLabel(), nodeLabelState.labels());
    }

    @Override
    int nodeTaskLabelDifference(NodeWithAlloc node, String taskName) {
        if (!nodeLabelsReady() || !taskIsKnown(taskName)) {
            return 0;
        }
        int taskLabel = taskLabels.get(taskName);
        int nodeLabel = nodeLabelState.labels().get(node);
        return Math.abs(nodeLabel - taskLabel);
    }

    @Override
    boolean taskIsKnown(String taskName) {
        return taskLabels.containsKey(taskName);
    }

    @Override
    boolean nodeLabelsReady() {
        return true;
    }

    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);

        log.info("Benchmark Tarema Scheduler: Pod {} terminated. Saving its trace...", pod.getName());
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Benchmark Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        traces.saveTaskTrace(task);
        log.info("Benchmark Tarema Scheduler: Pod {} trace saved.", pod.getName());

        recalculateTaskLabels();
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        taskLabels = TaskLabeller.taskLabels(traces, FloatField.CPU_PERCENTAGE, groupWeights); // TODO: maybe change the field

        long endTime = System.currentTimeMillis();
        log.info("Benchmark Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
    }


}
