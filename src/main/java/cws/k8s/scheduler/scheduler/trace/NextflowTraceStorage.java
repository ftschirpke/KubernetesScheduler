package cws.k8s.scheduler.scheduler.trace;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.Getter;

import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class NextflowTraceStorage {

    @Getter
    final private ArrayList<String> nodeNames;
    @Getter
    final private ArrayList<String> abstractTaskNames;

    final private ArrayList<Integer> nodeIds;
    final private ArrayList<Integer> abstractTaskIds;
    final private ArrayList<Integer> taskIds;
    final private ArrayList<Float> cpuPercentageValues;
    final private ArrayList<Long> rssValues;
    final private ArrayList<Long> vmemValues;
    final private ArrayList<Long> rcharValues;
    final private ArrayList<Long> wcharValues;
    final private ArrayList<Float> cpusValues;
    final private ArrayList<Long> memoryValues;
    final private ArrayList<Long> realtimeValues;

    public NextflowTraceStorage() {
        this.nodeNames = new ArrayList<>();
        this.abstractTaskNames = new ArrayList<>();
        this.nodeIds = new ArrayList<>();
        this.abstractTaskIds = new ArrayList<>();
        this.taskIds = new ArrayList<>();
        this.cpuPercentageValues = new ArrayList<>();
        this.rssValues = new ArrayList<>();
        this.vmemValues = new ArrayList<>();
        this.rcharValues = new ArrayList<>();
        this.wcharValues = new ArrayList<>();
        this.cpusValues = new ArrayList<>();
        this.memoryValues = new ArrayList<>();
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
        cpuPercentageValues.add(trace.getPercentageValue("%cpu"));
        rssValues.add(trace.getMemoryValue("rss"));
        vmemValues.add(trace.getMemoryValue("vmem"));
        rcharValues.add(trace.getMemoryValue("rchar"));
        wcharValues.add(trace.getMemoryValue("wchar"));
        cpusValues.add(config.getCpus());
        memoryValues.add(config.getMemoryInBytes());
        realtimeValues.add(trace.getTimeValue("realtime"));
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
    public Stream<Float> getCpuPercentageValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, cpuPercentageValues);
    }
    public Stream<Float> getCpuPercentageValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, cpuPercentageValues);
    }
    public Stream<Long> getRssValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, rssValues);
    }
    public Stream<Long> getRssValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, rssValues);
    }
    public Stream<Long> getVmemValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, vmemValues);
    }
    public Stream<Long> getVmemValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, vmemValues);
    }
    public Stream<Long> getRcharValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, rcharValues);
    }
    public Stream<Long> getRcharValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, rcharValues);
    }
    public Stream<Long> getWcharValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, wcharValues);
    }
    public Stream<Long> getWcharValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, wcharValues);
    }
    public Stream<Float> getCpusValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, cpusValues);
    }
    public Stream<Float> getCpusValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, cpusValues);
    }
    public Stream<Long> getMemoryValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, memoryValues);
    }
    public Stream<Long> getMemoryValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, memoryValues);
    }
    public Stream<Long> getRealtimeValuesForNode(String nodeName) {
        return getByIndex(getNodeIndex(nodeName), nodeIds, realtimeValues);
    }
    public Stream<Long> getRealtimeValuesForAbstractTask(String abstractTaskName) {
        return getByIndex(getAbstractTaskIndex(abstractTaskName), abstractTaskIds, realtimeValues);
    }
}
