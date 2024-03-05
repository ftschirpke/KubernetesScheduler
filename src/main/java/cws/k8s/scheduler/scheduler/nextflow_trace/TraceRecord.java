package cws.k8s.scheduler.scheduler.nextflow_trace;

import cws.k8s.scheduler.model.Task;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

@Slf4j
@ToString
public class TraceRecord {
    static final String TRACE_FILE_NAME = ".command.trace";
    static final String NEXTFLOW_TRACE_VERSION = "nextflow.trace/v2";
    static final String[] stringFields = {"cpu_model"};
    static final String[] timeFields = {"realtime"};
    static final String[] integerFields = {"syscr", "syscw", "vol_ctxt", "inv_ctxt"};
    static final String[] percentageFields = {"%cpu", "%mem"};
    static final String[] memoryFields = {
            "rss", "vmem", "peak_rss", "peak_vmem", "rchar", "wchar", "read_bytes", "write_bytes"
    };

    private final HashMap<String, String> stringEntries;
    private final HashMap<String, Long> timeEntries;
    private final HashMap<String, Integer> integerEntries;
    private final HashMap<String, Float> percentageEntries;
    private final HashMap<String, Long> memoryEntries;

    public TraceRecord() {
        this.stringEntries = new HashMap<>();
        this.timeEntries = new HashMap<>();
        this.integerEntries = new HashMap<>();
        this.percentageEntries = new HashMap<>();
        this.memoryEntries = new HashMap<>();
    }

    public void insertValue(String key, String value) throws IllegalArgumentException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        if (ArrayUtils.contains(stringFields, key)) {
            stringEntries.put(key, value);
        } else if (ArrayUtils.contains(timeFields, key)) {
            try {
                long valueAsLong = Long.parseLong(value);
                timeEntries.put(key, valueAsLong);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid value for %s: %s - should be a long", key, value)
                );
            }
        } else if (ArrayUtils.contains(integerFields, key)) {
            try {
                int valueAsInteger = Integer.parseInt(value);
                integerEntries.put(key, valueAsInteger);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid value for %s: %s - should be an integer", key, value)
                );
            }
        } else if (ArrayUtils.contains(percentageFields, key)) {
            try {
                float valueAsFloat = Float.parseFloat(value);
                percentageEntries.put(key, valueAsFloat);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid value for %s: %s - should be a float", key, value)
                );
            }
        } else if (ArrayUtils.contains(memoryFields, key)) {
            try {
                long valueAsLong = Long.parseLong(value);
                memoryEntries.put(key, valueAsLong);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Invalid value for %s: %s - should be a long", key, value)
                );
            }
        }
    }

    public static TraceRecord from_task(Task task) {
        TraceRecord traceRecord = new TraceRecord();
        Path path = Paths.get(task.getWorkingDir(), TRACE_FILE_NAME);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                if (line.equals(NEXTFLOW_TRACE_VERSION)) {
                    return;
                }
                String[] split = line.split("=");
                if (split.length != 2) {
                    log.warn("Trace file {} has invalid line: {} - SKIPPED", path, line);
                }
                try {
                    traceRecord.insertValue(split[0], split[1]);
                } catch (IllegalArgumentException e) {
                    log.warn("Trace file {} has invalid entry: {} - SKIPPED", path, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.warn("Error reading trace file {}", path, e);
        }
        return traceRecord;
    }

    public String getStringValue(String key) throws IllegalArgumentException {
        if (ArrayUtils.contains(stringFields, key)) {
            return stringEntries.get(key);
        } else {
            throw new IllegalArgumentException(String.format("Key %s is not a string field", key));
        }
    }

    public long getTimeValue(String key) throws IllegalArgumentException {
        if (ArrayUtils.contains(timeFields, key)) {
            Long value = timeEntries.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Key %s has no value", key));
            }
            return value;
        } else {
            throw new IllegalArgumentException(String.format("Key %s is not a time field", key));
        }
    }

    public int getIntegerValue(String key) throws IllegalArgumentException {
        if (ArrayUtils.contains(integerFields, key)) {
            Integer value = integerEntries.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Key %s has no value", key));
            }
            return value;
        } else {
            throw new IllegalArgumentException(String.format("Key %s is not an integer field", key));
        }
    }

    public float getPercentageValue(String key) throws IllegalArgumentException {
        if (ArrayUtils.contains(percentageFields, key)) {
            Float value = percentageEntries.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Key %s has no value", key));
            }
            return value;
        } else {
            throw new IllegalArgumentException(String.format("Key %s is not a percentage field", key));
        }
    }

    public long getMemoryValue(String key) throws IllegalArgumentException {
        if (ArrayUtils.contains(memoryFields, key)) {
            Long value = memoryEntries.get(key);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Key %s has no value", key));
            }
            return value;
        } else {
            throw new IllegalArgumentException(String.format("Key %s is not a memory field", key));
        }
    }
}
