package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;

public interface Approach {

    String getName();

    void onTaskTermination(TraceRecord trace, TaskConfig config, NodeWithAlloc node);

    void recalculate();
    void printNodeLabels();
    void printTaskLabels();
}
