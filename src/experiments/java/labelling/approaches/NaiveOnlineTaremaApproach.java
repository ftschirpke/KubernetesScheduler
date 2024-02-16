package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;

import java.util.stream.Stream;

/*
 * This class represents the Naive Online Tarema approach to labelling tasks.
 * The Naive approach retrieves node labels from task traces through a regression model at runtime.
 * The task labelling is naively copied from the original Tarema approach.
 */
public class NaiveOnlineTaremaApproach {
    int nextTaskId = 0;
    NextflowTraceStorage traceStorage;
    NodeLabeller nodeLabeller;
    TaskLabeller taskLabeller;

    public NaiveOnlineTaremaApproach() {
        this.taskLabeller = new TaskLabeller();
        this.nodeLabeller = new NodeLabeller();
    }

    public void initialize() {
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        nodeLabeller.recalculateLabels(traceStorage, Stream.of(node));
        taskLabeller.recalculateLabels(traceStorage, nodeLabeller.getGroupWeights());
    }
}
