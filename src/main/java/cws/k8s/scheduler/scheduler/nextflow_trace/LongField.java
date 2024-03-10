package cws.k8s.scheduler.scheduler.nextflow_trace;

import java.util.List;

public enum LongField implements TraceField<Long> {
    MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.memoryValues;
        }

        @Override
        public String toString() {
            return "memory";
        }
    },
    RESIDENT_SET_SIZE {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.rssValues;
        }

        @Override
        public String toString() {
            return "rss";
        }
    },
    VIRTUAL_MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.vmemValues;
        }

        @Override
        public String toString() {
            return "vmem";
        }
    },
    PEAK_RESIDENT_SET_SIZE {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.peakRssValues;
        }

        @Override
        public String toString() {
            return "peak_rss";
        }
    },
    PEAK_VIRTUAL_MEMORY {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.peakVmemValues;
        }

        @Override
        public String toString() {
            return "peak_vmem";
        }
    },
    CHARACTERS_READ {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.rcharValues;
        }

        @Override
        public String toString() {
            return "rchar";
        }
    },
    CHARACTERS_WRITTEN {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.wcharValues;
        }

        @Override
        public String toString() {
            return "wchar";
        }
    },
    BYTES_READ{
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.readBytesValues;
        }

        @Override
        public String toString() {
            return "read_bytes";
        }
    },
    BYTES_WRITTEN {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.writeBytesValues;
        }

        @Override
        public String toString() {
            return "write_bytes";
        }
    },
    REALTIME {
        @Override
        public List<Long> getValuesFromStorage(TraceStorage storage) {
            return storage.realtimeValues;
        }

        @Override
        public String toString() {
            return "realtime";
        }
    },
}
