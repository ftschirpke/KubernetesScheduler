package labelling.approaches;

import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nextflow_trace.TraceRecord;

import java.util.List;

public interface Approach {

    String getName();

    void onTaskTermination(TraceRecord trace, TaskConfig config, String node);

    void recalculate();
    void printNodeLabels();
    void printTaskLabels();

    void writeState(String experimentDir, List<String> taskNames);
}
