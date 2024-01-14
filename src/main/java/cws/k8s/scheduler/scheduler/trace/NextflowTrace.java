package cws.k8s.scheduler.scheduler.trace;

public class NextflowTrace {
    public static enum FloatFields {
        CPUS,
        CPU_PERCENTAGE,
        MEMORY_PERCENTAGE,
    }
    public static enum LongFields {
        REALTIME,
        MEMORY,
        RESIDENT_SET_SIZE,
        VIRTUAL_MEMORY,
        PEAK_RESIDENT_SET_SIZE,
        PEAK_VIRTUAL_MEMORY,
        CHARACTERS_READ,
        CHARACTERS_WRITTEN,
        BYTES_READ,
        BYTES_WRITTEN,
    }
    public static enum IntegerFields {
        READ_SYSCALLS,
        WRITE_SYSCALLS,
        VOLUNTARY_CONTEXT_SWITCHES,
        INVOLUNTARY_CONTEXT_SWITCHES,
    }
}
