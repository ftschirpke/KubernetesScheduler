package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.LotaruTraces;


/*
 * This class represents the Tarema approach to labelling tasks.
 * The Tarema approach retrieves node labels from benchmarks before the scheduling process starts.
 * At runtime, only the task labels are updated.
 */
public class TaremaApproach implements Approach {
    int nextTaskId = 0;
    NextflowTraceStorage traceStorage;

    NodeLabeller.NodeLabelState fixedNodeLabels;

    TaskLabeller taskLabeller;

    public TaremaApproach() {
        this.taskLabeller = new TaskLabeller();
    }

    public void initialize() {
        fixedNodeLabels = NodeLabeller.labelOnce(LotaruTraces.lotaruBenchmarkResults);
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;

        taskLabeller.recalculateLabels(traceStorage, fixedNodeLabels.groupWeights());
    }
}
