package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public enum LongField implements TraceField<Long> {
    MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.memoryValues;
        }
    },
    RESIDENT_SET_SIZE {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.rssValues;
        }
    },
    VIRTUAL_MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.vmemValues;
        }
    },
    PEAK_RESIDENT_SET_SIZE {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.peakRssValues;
        }
    },
    PEAK_VIRTUAL_MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.peakVmemValues;
        }
    },
    CHARACTERS_READ {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.rcharValues;
        }
    },
    CHARACTERS_WRITTEN {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.wcharValues;
        }
    },
    BYTES_READ{
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.readBytesValues;
        }
    },
    BYTES_WRITTEN {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.writeBytesValues;
        }
    },
    REALTIME {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.realtimeValues;
        }
    },
}
