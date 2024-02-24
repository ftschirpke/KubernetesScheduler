package labelling.approaches;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;

import java.util.Map;

public interface Approach {
    void initialize();
    void onTaskTermination(NextflowTraceRecord trace, TaskConfig config, NodeWithAlloc node);

    void recalculate();
    Map<NodeWithAlloc, Labels> getNodeLabels();
}
