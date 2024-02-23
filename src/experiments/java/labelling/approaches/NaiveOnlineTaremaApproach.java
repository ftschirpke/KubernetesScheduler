package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.GroupWeights;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;

import java.util.Map;
import java.util.stream.Stream;

/*
 * This class represents the Naive Online Tarema approach to labelling tasks.
 * The Naive approach retrieves node labels from task traces through a regression model at runtime.
 * The task labelling is naively copied from the original Tarema approach.
 */
public class NaiveOnlineTaremaApproach implements Approach {
    int nextTaskId = 0;
    NextflowTraceStorage traceStorage;
    NodeFirstLabeller nodeFirstLabeller;
    TaskSecondLabeller taskSecondLabeller;

    public NaiveOnlineTaremaApproach() {
        this.traceStorage = new NextflowTraceStorage();
        this.taskSecondLabeller = new TaskSecondLabeller();
        this.nodeFirstLabeller = new NodeFirstLabeller();
    }

    public void initialize() {
    }

    public Map<NodeWithAlloc, Labels> getNodeLabels() {
        return nodeFirstLabeller.getLabels();
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        nodeFirstLabeller.recalculateLabels(traceStorage, Stream.of(node));

        Labels maxNodeLabels = nodeFirstLabeller.getMaxLabels();
        Map<NodeWithAlloc, Labels> nodeLabels = nodeFirstLabeller.getLabels();
        GroupWeights groupWeights = GroupWeights.forNodeLabels(maxNodeLabels, nodeLabels);

        taskSecondLabeller.recalculateLabels(traceStorage, groupWeights);
    }
}
