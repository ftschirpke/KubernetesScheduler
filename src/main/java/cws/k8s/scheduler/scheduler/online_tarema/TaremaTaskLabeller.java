package cws.k8s.scheduler.scheduler.online_tarema;

import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;

@Getter
@Slf4j
public class TaremaTaskLabeller {

    final private HashMap<String, ArrayList<String>> namesByTask;
    final private HashMap<String, ArrayList<Integer>> idsByTask;
    final private HashMap<String, ArrayList<Float>> cpuPercentageValuesByTask;
    final private HashMap<String, ArrayList<Long>> rssValuesByTask;
    final private HashMap<String, ArrayList<Long>> vmemValuesByTask;
    final private HashMap<String, ArrayList<Long>> rcharValuesByTask;
    final private HashMap<String, ArrayList<Long>> wcharValuesByTask;
    final private HashMap<String, ArrayList<Float>> cpusValuesByTask;
    final private HashMap<String, ArrayList<Long>> memoryValuesByTask;
    final private HashMap<String, ArrayList<Long>> realtimeValuesByTask;

    public TaremaTaskLabeller() {
        this.namesByTask = new HashMap<>();
        this.idsByTask = new HashMap<>();
        this.cpuPercentageValuesByTask = new HashMap<>();
        this.rssValuesByTask = new HashMap<>();
        this.vmemValuesByTask = new HashMap<>();
        this.rcharValuesByTask = new HashMap<>();
        this.wcharValuesByTask = new HashMap<>();
        this.cpusValuesByTask = new HashMap<>();
        this.memoryValuesByTask = new HashMap<>();
        this.realtimeValuesByTask = new HashMap<>();
    }

    public void saveTaskTrace(Task task) {
        NextflowTraceRecord trace = NextflowTraceRecord.from_task(task);
        TaskConfig config = task.getConfig();
        String abstractTaskName = config.getTask();

        String name = config.getName();
        namesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(name);
        int id = task.getId();
        idsByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(id);
        float cpuPercentage = trace.getPercentageValue("%cpu");
        cpuPercentageValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(cpuPercentage);
        long rss = trace.getMemoryValue("rss");
        rssValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(rss);
        long vmem = trace.getMemoryValue("vmem");
        vmemValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(vmem);
        long rchar = trace.getMemoryValue("rchar");
        rcharValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(rchar);
        long wchar = trace.getMemoryValue("wchar");
        wcharValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(wchar);
        float cpus = config.getCpus();
        cpusValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(cpus);
        long memory = config.getMemoryInBytes();
        memoryValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(memory);
        long realtime = trace.getTimeValue("realtime");
        realtimeValuesByTask.getOrDefault(abstractTaskName, new ArrayList<>()).add(realtime);

        log.info("Online Tarema Scheduler: Successfully saved trace for task {}", task);
    }

    public HashMap<String, Labels> determineLabels(int labelSpaceSize) {
        return new HashMap<>();
    }

}
