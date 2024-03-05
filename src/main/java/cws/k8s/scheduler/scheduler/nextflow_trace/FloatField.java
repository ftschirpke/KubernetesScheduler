package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public enum FloatField implements TraceField<Float> {
    CPUS {
        @Override
        public List<Float> getValuesFromStorage(TraceStorage storage) {
            return storage.cpusValues;
        }
    },
    CPU_PERCENTAGE {
        @Override
        public List<Float> getValuesFromStorage(TraceStorage storage) {
            return storage.cpuPercentageValues;
        }
    },
    MEMORY_PERCENTAGE {
        @Override
        public List<Float> getValuesFromStorage(TraceStorage storage) {
            return storage.memoryPercentageValues;
        }
    },
}
