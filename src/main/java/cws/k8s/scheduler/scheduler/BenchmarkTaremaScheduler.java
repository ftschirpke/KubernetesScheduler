package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class BenchmarkTaremaScheduler extends TaremaScheduler {
    private final NextflowTraceStorage traces = new NextflowTraceStorage();
    private final NodeFirstLabeller.NodeLabelState nodeLabelState;
    private final TaskSecondLabeller taskLabeller = new TaskSecondLabeller();

    public BenchmarkTaremaScheduler(
            Map<NodeWithAlloc, NodeFirstLabeller.NodeSpeedEstimation> fixedNodeSpeedEstimations,
            String execution,
            KubernetesClient client,
            String namespace,
            SchedulerConfig config) throws IOException {
        super(execution, client, namespace, config);
        nodeLabelState = NodeFirstLabeller.labelOnce(fixedNodeSpeedEstimations);
        assert nodeLabelState.maxLabels() != null;
        assert nodeLabelState.labels() != null;
        assert nodeLabelState.groupWeights() != null;
    }

    @Override
    @NotNull
    Map<NodeWithAlloc, Labels> getNodeLabels() {
        return nodeLabelState.labels();
    }

    @Override
    @NotNull
    Map<String, Labels> getTaskLabels() {
        return taskLabeller.getLabels();
    }

    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);

        log.info("Online Tarema Scheduler: Pod {} terminated. Saving its trace...", pod.getName());
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Online Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        traces.saveTaskTrace(task);
        log.info("Online Tarema Scheduler: Pod {} trace saved.", pod.getName());

        recalculateTaskLabels();
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        taskLabeller.recalculateLabels(traces, nodeLabelState.groupWeights());

        long endTime = System.currentTimeMillis();
        log.info("Benchmark Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
        log.info("Benchmark Tarema Scheduler: New task labels are:\n{}", taskLabeller.getLabels());
    }


}
