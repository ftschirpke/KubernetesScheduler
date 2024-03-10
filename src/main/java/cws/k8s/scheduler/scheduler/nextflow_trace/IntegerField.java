package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public enum IntegerField implements TraceField<Integer> {
    READ_SYSCALLS {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.syscrValues;
        }

        @Override
        public String toString() {
            return "syscr";
        }
    },
    WRITE_SYSCALLS {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.syscwValues;
        }

        @Override
        public String toString() {
            return "syscw";
        }
    },
    VOLUNTARY_CONTEXT_SWITCHES {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.volCtxtValues;
        }

        @Override
        public String toString() {
            return "vol_ctxt";
        }
    },
    INVOLUNTARY_CONTEXT_SWITCHES {
        @Override
        public List<Integer> getValuesFromStorage(TraceStorage storage) {
            return storage.invCtxtValues;
        }

        @Override
        public String toString() {
            return "inv_ctxt";
        }
    },
}
