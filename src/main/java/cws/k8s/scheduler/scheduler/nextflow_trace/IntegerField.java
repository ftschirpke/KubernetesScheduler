package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public enum IntegerField implements TraceField<Integer> {
    READ_SYSCALLS {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.syscrValues;
        }
    },
    WRITE_SYSCALLS {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.syscwValues;
        }
    },
    VOLUNTARY_CONTEXT_SWITCHES {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.volCtxtValues;
        }
    },
    INVOLUNTARY_CONTEXT_SWITCHES {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.invCtxtValues;
        }
    },
}
