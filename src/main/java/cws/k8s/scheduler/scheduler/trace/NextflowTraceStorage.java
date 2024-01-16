package cws.k8s.scheduler.scheduler.trace;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.Getter;

import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NextflowTraceStorage {

    public enum FloatField {
        CPUS,
        CPU_PERCENTAGE,
        MEMORY_PERCENTAGE,
    }

    public enum IntegerField {
        READ_SYSCALLS,
        WRITE_SYSCALLS,
        VOLUNTARY_CONTEXT_SWITCHES,
        INVOLUNTARY_CONTEXT_SWITCHES,
    }

    public enum LongField {
        MEMORY,
        RESIDENT_SET_SIZE,
        VIRTUAL_MEMORY,
        PEAK_RESIDENT_SET_SIZE,
        PEAK_VIRTUAL_MEMORY,
        CHARACTERS_READ,
        CHARACTERS_WRITTEN,
        BYTES_READ,
        BYTES_WRITTEN,
        REALTIME,
    }

    @Getter
    final private ArrayList<String> nodeNames;
    @Getter
    final private ArrayList<String> abstractTaskNames;

    final private ArrayList<Integer> nodeIds;
    final private ArrayList<Integer> abstractTaskIds;

    final private ArrayList<Integer> taskIds;
    final private ArrayList<Float> cpusValues;
    final private ArrayList<Float> cpuPercentageValues;
    final private ArrayList<Float> memoryPercentageValues;
    final private ArrayList<Integer> syscrValues;
    final private ArrayList<Integer> syscwValues;
    final private ArrayList<Integer> volCtxtValues;
    final private ArrayList<Integer> invCtxtValues;
    final private ArrayList<Long> memoryValues;
    final private ArrayList<Long> rssValues;
    final private ArrayList<Long> vmemValues;
    final private ArrayList<Long> peakRssValues;
    final private ArrayList<Long> peakVmemValues;
    final private ArrayList<Long> rcharValues;
    final private ArrayList<Long> wcharValues;
    final private ArrayList<Long> readBytesValues;
    final private ArrayList<Long> writeBytesValues;
    final private ArrayList<Long> realtimeValues;

    public NextflowTraceStorage() {
        this.nodeNames = new ArrayList<>();
        this.abstractTaskNames = new ArrayList<>();
        this.nodeIds = new ArrayList<>();
        this.abstractTaskIds = new ArrayList<>();
        this.taskIds = new ArrayList<>();
        this.cpusValues = new ArrayList<>();
        this.cpuPercentageValues = new ArrayList<>();
        this.memoryPercentageValues = new ArrayList<>();
        this.syscrValues = new ArrayList<>();
        this.syscwValues = new ArrayList<>();
        this.volCtxtValues = new ArrayList<>();
        this.invCtxtValues = new ArrayList<>();
        this.memoryValues = new ArrayList<>();
        this.rssValues = new ArrayList<>();
        this.vmemValues = new ArrayList<>();
        this.peakRssValues = new ArrayList<>();
        this.peakVmemValues = new ArrayList<>();
        this.rcharValues = new ArrayList<>();
        this.wcharValues = new ArrayList<>();
        this.readBytesValues = new ArrayList<>();
        this.writeBytesValues = new ArrayList<>();
        this.realtimeValues = new ArrayList<>();
    }

    private int getNodeIndex(String nodeName) {
        int index = nodeNames.indexOf(nodeName);
        if (index == -1) {
            nodeNames.add(nodeName);
            index = nodeNames.size() - 1;
        }
        return index;
    }

    private int getAbstractTaskIndex(String abstractTaskName) {
        int index = abstractTaskNames.indexOf(abstractTaskName);
        if (index == -1) {
            abstractTaskNames.add(abstractTaskName);
            index = abstractTaskNames.size() - 1;
        }
        return index;
    }

    public void saveTaskTrace(Task task) {
        NextflowTraceRecord trace = NextflowTraceRecord.from_task(task);
        TaskConfig config = task.getConfig();

        String nodeName = task.getNode().getName();
        int nodeIndex = getNodeIndex(nodeName);
        nodeIds.add(nodeIndex);
        String abstractTaskName = config.getTask();
        int abstractTaskIndex = getAbstractTaskIndex(abstractTaskName);
        abstractTaskIds.add(abstractTaskIndex);

        taskIds.add(task.getId());
        cpusValues.add(config.getCpus());
        memoryValues.add(config.getMemoryInBytes());
        cpuPercentageValues.add(trace.getPercentageValue("%cpu"));
        memoryPercentageValues.add(trace.getPercentageValue("%mem"));
        syscrValues.add(trace.getIntegerValue("syscr"));
        syscwValues.add(trace.getIntegerValue("syscw"));
        volCtxtValues.add(trace.getIntegerValue("vol_ctxt"));
        invCtxtValues.add(trace.getIntegerValue("inv_ctxt"));
        rssValues.add(trace.getMemoryValue("rss"));
        vmemValues.add(trace.getMemoryValue("vmem"));
        peakRssValues.add(trace.getMemoryValue("peak_rss"));
        peakVmemValues.add(trace.getMemoryValue("peak_vmem"));
        rcharValues.add(trace.getMemoryValue("rchar"));
        wcharValues.add(trace.getMemoryValue("wchar"));
        readBytesValues.add(trace.getMemoryValue("read_bytes"));
        writeBytesValues.add(trace.getMemoryValue("write_bytes"));
        realtimeValues.add(trace.getTimeValue("realtime"));
    }

    public ArrayList<Float> getAll(FloatField field) {
        return switch (field) {
            case CPUS -> cpusValues;
            case CPU_PERCENTAGE -> cpuPercentageValues;
            case MEMORY_PERCENTAGE -> memoryPercentageValues;
        };
    }

    public ArrayList<Integer> getAll(IntegerField field) {
        return switch (field) {
            case READ_SYSCALLS -> syscrValues;
            case WRITE_SYSCALLS -> syscwValues;
            case VOLUNTARY_CONTEXT_SWITCHES -> volCtxtValues;
            case INVOLUNTARY_CONTEXT_SWITCHES -> invCtxtValues;
        };
    }

    public ArrayList<Long> getAll(LongField field) {
        return switch (field) {
            case MEMORY -> memoryValues;
            case RESIDENT_SET_SIZE -> rssValues;
            case VIRTUAL_MEMORY -> vmemValues;
            case PEAK_RESIDENT_SET_SIZE -> peakRssValues;
            case PEAK_VIRTUAL_MEMORY -> peakVmemValues;
            case CHARACTERS_READ -> rcharValues;
            case CHARACTERS_WRITTEN -> wcharValues;
            case BYTES_READ -> readBytesValues;
            case BYTES_WRITTEN -> writeBytesValues;
            case REALTIME -> realtimeValues;
        };
    }

    private static <T> Stream<T> getByIndex(int index, ArrayList<Integer> indexList, ArrayList<T> data) {
        return IntStream.range(0, indexList.size())
                .filter(i -> indexList.get(i) == index)
                .mapToObj(data::get);
    }

    public Stream<Integer> getTaskIdsForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, taskIds);
    }
    public Stream<Integer> getTaskIdsForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, taskIds);
    }

    public Stream<Float> getForNode(String nodeName, FloatField field) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, getAll(field));
    }
    public Stream<Float> getForAbstractTask(String abstractTaskName, FloatField field) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, getAll(field));
    }

    public Stream<Integer> getForNode(String nodeName, IntegerField field) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, getAll(field));
    }
    public Stream<Integer> getForAbstractTask(String abstractTaskName, IntegerField field) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, getAll(field));
    }

    public Stream<Long> getForNode(String nodeName, LongField field) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, getAll(field));
    }
    public Stream<Long> getForAbstractTask(String abstractTaskName, LongField field) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, getAll(field));
    }


}
