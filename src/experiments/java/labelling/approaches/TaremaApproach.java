package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeFirstLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskSecondLabeller;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import labelling.LotaruTraces;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/*
 * This class represents the Tarema approach.
 * The Tarema approach retrieves node labels from benchmarks before the scheduling process starts.
 * At runtime, only the task labels are updated.
 */
@Slf4j
public class TaremaApproach implements Approach {
    int nextTaskId = 0;
    private final NextflowTraceStorage traceStorage = new NextflowTraceStorage();

    private NodeFirstLabeller.NodeLabelState fixedNodeLabels = null;

    private final TaskSecondLabeller taskSecondLabeller = new TaskSecondLabeller();

    public void initialize() {
        try {
            fixedNodeLabels = NodeFirstLabeller.labelOnce(LotaruTraces.lotaruBenchmarkResults);
        } catch (IOException e) {
            log.error("Failed to load benchmark results", e);
        }
    }

    public Map<NodeWithAlloc, Labels> getNodeLabels() {
        return fixedNodeLabels.labels();
    }

    public void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node) {
        traceStorage.saveTrace(trace, nextTaskId, config, node);
        nextTaskId++;
    }

    public void recalculate() {
        taskSecondLabeller.recalculateLabels(traceStorage, fixedNodeLabels.groupWeights());
    }
}
