package cws.k8s.scheduler.scheduler.nextflow_trace;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TraceStorage {

    @Getter
    private final ArrayList<String> nodes = new ArrayList<>();
    @Getter
    private final ArrayList<String> abstractTaskNames = new ArrayList<>();

    private final ArrayList<Integer> nodeIndices = new ArrayList<>();
    private final ArrayList<Integer> abstractTaskIndices = new ArrayList<>();

    private final ArrayList<Integer> taskIds = new ArrayList<>();

    final ArrayList<Float> cpusValues = new ArrayList<>();
    final ArrayList<Float> cpuPercentageValues = new ArrayList<>();
    final ArrayList<Float> memoryPercentageValues = new ArrayList<>();
    final ArrayList<Integer> syscrValues = new ArrayList<>();
    final ArrayList<Integer> syscwValues = new ArrayList<>();
    final ArrayList<Integer> volCtxtValues = new ArrayList<>();
    final ArrayList<Integer> invCtxtValues = new ArrayList<>();
    final ArrayList<Long> memoryValues = new ArrayList<>();
    final ArrayList<Long> rssValues = new ArrayList<>();
    final ArrayList<Long> vmemValues = new ArrayList<>();
    final ArrayList<Long> peakRssValues = new ArrayList<>();
    final ArrayList<Long> peakVmemValues = new ArrayList<>();
    final ArrayList<Long> rcharValues = new ArrayList<>();
    final ArrayList<Long> wcharValues = new ArrayList<>();
    final ArrayList<Long> readBytesValues = new ArrayList<>();
    final ArrayList<Long> writeBytesValues = new ArrayList<>();
    final ArrayList<Long> realtimeValues = new ArrayList<>();

    private int getNodeIndex(String nodeName) {
        int index = nodes.indexOf(nodeName);
        if (index == -1) {
            nodes.add(nodeName);
            index = nodes.size() - 1;
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

    public boolean empty() {
        return taskIds.isEmpty();
    }

    public int size() {
        return taskIds.size();
    }

    public synchronized Optional<Integer> saveTaskTrace(Task task) {
        int taskId = task.getId();
        if (taskIds.contains(taskId)) {
            return Optional.empty();
        }
        TraceRecord trace = TraceRecord.from_task(task);
        TaskConfig config = task.getConfig();
        String nodeName = task.getNode().getName();
        return saveTrace(trace, taskId, config, nodeName);
    }

    public Optional<Integer> saveTrace(TraceRecord trace, int taskId, TaskConfig config, String nodeName) {
        if (taskIds.contains(taskId)) {
            return Optional.empty();
        }
        int nodeIndex = getNodeIndex(nodeName);
        nodeIndices.add(nodeIndex);
        String abstractTaskName = config.getTask();
        int abstractTaskIndex = getAbstractTaskIndex(abstractTaskName);
        abstractTaskIndices.add(abstractTaskIndex);

        int index = taskIds.size();
        taskIds.add(taskId);
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
        long realtime = trace.getTimeValue("realtime");
        long nonZeroRealtime = Math.max(realtime, 1L);
        realtimeValues.add(nonZeroRealtime);
        return Optional.of(index);
    }

    private static <T> Stream<T> getByIndex(int index, List<Integer> indexList, List<T> data) {
        return IntStream.range(0, indexList.size())
                .filter(i -> indexList.get(i) == index)
                .mapToObj(data::get);
    }

    public <T> List<T> getAll(TraceField<T> field) {
        return field.getValuesFromStorage(this);
    }

    public Stream<Integer> getTaskIdsForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIndices, taskIds);
    }

    public Stream<Integer> getTaskIdsForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIndices, taskIds);
    }

    public <T> Stream<T> getForNode(String nodeName, TraceField<T> field) {
        return getByIndex(getNodeIndex(nodeName), nodeIndices, field.getValuesFromStorage(this));
    }

    public <T> T getForId(int id, TraceField<T> field) {
        return field.getValuesFromStorage(this).get(id);
    }

    public <T> Stream<T> getForAbstractTask(String abstractTaskName, TraceField<T> field) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIndices, field.getValuesFromStorage(this));
    }
}
