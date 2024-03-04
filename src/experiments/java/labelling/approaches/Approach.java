package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;

public interface Approach {

    String getName();

    void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node);

    void recalculate();
    void printNodeLabels();
    void printTaskLabels();
}
