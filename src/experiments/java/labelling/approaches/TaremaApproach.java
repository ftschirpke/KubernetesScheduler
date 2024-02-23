package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.LotaruTraces;

import java.util.Map;

/*
 * This class represents the Tarema approach.
 * The Tarema approach retrieves node labels from benchmarks before the scheduling process starts.
 * At runtime, only the task labels are updated.
 */
public class TaremaApproach implements Approach {
    int nextTaskId = 0;
    NextflowTraceStorage traceStorage;

    NodeFirstLabeller.NodeLabelState fixedNodeLabels;

    TaskSecondLabeller taskSecondLabeller;

    public TaremaApproach() {
        this.traceStorage = new NextflowTraceStorage();
        this.taskSecondLabeller = new TaskSecondLabeller();
    }

    public void initialize() {
        fixedNodeLabels = NodeFirstLabeller.labelOnce(LotaruTraces.lotaruBenchmarkResults);
    }

    public Map<NodeWithAlloc, Labels> getNodeLabels() {
        return fixedNodeLabels.labels();
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        taskSecondLabeller.recalculateLabels(traceStorage, fixedNodeLabels.groupWeights());
    }
}
