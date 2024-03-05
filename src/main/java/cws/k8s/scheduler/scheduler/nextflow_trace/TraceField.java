package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public interface TraceField<T> {
    List<T> getValuesFromStorage(TraceStorage storage);
}
