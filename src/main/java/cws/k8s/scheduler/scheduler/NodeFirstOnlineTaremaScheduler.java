package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class NodeFirstOnlineTaremaScheduler extends TaremaScheduler {
    static final long TEN_SECONDS = 10000;
    static final int MIN_TASKS_FOR_NODE_LABELLING = 2;


    private final NextflowTraceStorage traces = new NextflowTraceStorage();
    private final NodeFirstLabeller nodeLabeller = new NodeFirstLabeller();
    private final TaskSecondLabeller taskLabeller = new TaskSecondLabeller();

    private final Map<NodeWithAlloc, List<Integer>> nodesWithNewData = new HashMap<>();
    private long lastNodeLabelsRecalculation = 0;

    public NodeFirstOnlineTaremaScheduler(String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config) {
        super(execution, client, namespace, config);
    }

    @Override
    @NotNull
    Map<NodeWithAlloc, Labels> getNodeLabels() {
        return nodeLabeller.getLabels();
    }

    @Override
    @NotNull
    Map<String, Labels> getTaskLabels() {
        return taskLabeller.getLabels();
    }

    @Override
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

        NodeWithAlloc node = task.getNode();
        // only calculate node labels if all nodes have data
        // which is already the case if all nodes are already labelled
        boolean allNodesAlreadyLabelled = traces.getNodes().size() == availableNodes.size();
        boolean allNodesHaveData = nodesWithNewData.size() == availableNodes.size();
        if (allNodesAlreadyLabelled || allNodesHaveData) {
            long finishedTasksOnNode = traces.getTaskIdsForNode(node).count();
            if (finishedTasksOnNode >= MIN_TASKS_FOR_NODE_LABELLING) {
                nodesWithNewData.putIfAbsent(node, new ArrayList<>());
                nodesWithNewData.get(node).add(task.getId());
            }
            if (System.currentTimeMillis() - lastNodeLabelsRecalculation > TEN_SECONDS) {
                recalculateNodeLabels(nodesWithNewData);
                nodesWithNewData.clear();
            }
        }
        recalculateTaskLabels();
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        GroupWeights groupWeights = GroupWeights.forNodeLabels(nodeLabeller.getMaxLabels(), nodeLabeller.getLabels());
        if (groupWeights == null) {
            log.info("Online Tarema Scheduler: No group weights available to recalculate task labels.");
            return;
        }
        taskLabeller.recalculateLabels(traces, groupWeights);

        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
        log.info("Online Tarema Scheduler: New task labels are:\n{}", taskLabeller.getLabels());
    }

    public void recalculateNodeLabels(Map<NodeWithAlloc, List<Integer>> nodesWithNewData) {
        long startTime = System.currentTimeMillis();
        boolean changed = nodeLabeller.recalculateLabels(traces, nodesWithNewData);
        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Node labels recalculated in {} ms.", endTime - startTime);
        lastNodeLabelsRecalculation = endTime;
        if (!changed) {
            return;
        }
        log.info("Online Tarema Scheduler: New node labels are:\n{}", nodeLabeller.getLabels());
    }
}
