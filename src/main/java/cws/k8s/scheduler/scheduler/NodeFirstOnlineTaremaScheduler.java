package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RandomNodeAssign;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.scheduler.prioritize.Prioritize;
import cws.k8s.scheduler.scheduler.prioritize.RankMinPrioritize;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class NodeFirstOnlineTaremaScheduler extends TaremaScheduler {
    static final long TEN_SECONDS = 10000;
    static final int MIN_TASKS_FOR_NODE_LABELLING = 2;


    private final NextflowTraceStorage traces = new NextflowTraceStorage();
    private final NodeFirstLabeller nodeLabeller = new NodeFirstLabeller();
    private final TaskSecondLabeller taskLabeller = new TaskSecondLabeller();

    private final List<NodeWithAlloc> nodesWithNewData = new ArrayList<>();
    private long lastNodeLabelsRecalculation = 0;

    public NodeFirstOnlineTaremaScheduler(String execution,
                                          KubernetesClient client,
                                          String namespace,
                                          SchedulerConfig config) {
        super(execution, client, namespace, config);
    }

    @Override
    Map<NodeWithAlloc, Labels> getNodeLabels() {
        return nodeLabeller.getLabels();
    }

    @Override
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
                nodesWithNewData.add(node);
            }
            if (System.currentTimeMillis() - lastNodeLabelsRecalculation > TEN_SECONDS) {
                recalculateNodeLabels(nodesWithNewData.stream());
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

    public void recalculateNodeLabels(Stream<NodeWithAlloc> nodesWithNewData) {
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
