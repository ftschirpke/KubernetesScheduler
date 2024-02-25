package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/*
 * This class represents the Naive Online Tarema approach to labelling tasks.
 * The Naive approach retrieves node labels from task traces through a regression model at runtime.
 * The task labelling is naively copied from the original Tarema approach.
 */
public class NaiveOnlineTaremaApproach implements Approach {
    private int nextTaskId = 0;
    private final NextflowTraceStorage traceStorage = new NextflowTraceStorage();
    private final NodeFirstLabeller nodeFirstLabeller = new NodeFirstLabeller();
    private final TaskSecondLabeller taskSecondLabeller = new TaskSecondLabeller();
    private final Map<NodeWithAlloc, List<Integer>> nodesWithNewData = new HashMap<>();

    public void initialize() {
    }

    public Map<NodeWithAlloc, Labels> getNodeLabels() {
        return nodeFirstLabeller.getLabels();
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        int id = traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;
        nodesWithNewData.putIfAbsent(node, new ArrayList<>());
        nodesWithNewData.get(node).add(id);
    }

    public void recalculate() {
        nodeFirstLabeller.recalculateLabels(traceStorage, nodesWithNewData);

        Labels maxNodeLabels = nodeFirstLabeller.getMaxLabels();
        Map<NodeWithAlloc, Labels> nodeLabels = nodeFirstLabeller.getLabels();
        GroupWeights groupWeights = GroupWeights.forNodeLabels(maxNodeLabels, nodeLabels);
        if (groupWeights == null) {
            return;
        }

        taskSecondLabeller.recalculateLabels(traceStorage, groupWeights);
    }
}
