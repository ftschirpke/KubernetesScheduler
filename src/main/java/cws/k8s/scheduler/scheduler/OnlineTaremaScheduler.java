package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import cws.k8s.scheduler.scheduler.nodeassign.OnlineTaremaAssign;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceRecord;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
public class OnlineTaremaScheduler extends PrioritizeAssignScheduler {

    private record TaskValues (
        String task_name,
        int task_id,
        float cpu_percentage,
        long rss,
        long vmem,
        long rchar,
        long wchar,
        float cpus,
        long memory,
        long realtime,
        String run_name
    ) {}

    final private HashMap<String, ArrayList<TaskValues>> valuesByAbstractTask = new HashMap<>();

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config) {
        super(execution, client, namespace, config, new MinInputPrioritize(), new OnlineTaremaAssign());
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod) {
        log.info("Online Tarema Scheduler: Received Event {} for pod {}.", action, pod);
    }

    @Override
    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);
        log.info("Online Tarema Scheduler: Pod {} terminated. Saving trace...", pod);
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Online Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod);
            return;
        }
        NextflowTraceRecord trace = NextflowTraceRecord.from_task(task);
        TaskConfig config = task.getConfig();
        TaskValues values = new TaskValues(
                config.getName(),
                task.getId(),
                trace.getPercentageValue("%cpu"),
                trace.getMemoryValue("rss"),
                trace.getMemoryValue("vmem"),
                trace.getMemoryValue("rchar"),
                trace.getMemoryValue("wchar"),
                config.getCpus(),
                config.getMemoryInBytes(),
                trace.getTimeValue("realtime"),
                config.getRunName()
        );
        log.info("Online Tarema Scheduler: Got trace values: {}", values);
        valuesByAbstractTask.getOrDefault(config.getTask(), new ArrayList<>()).add(values);
        log.info("Online Tarema Scheduler: Successfully saved trace for task {} (pod {})", task, pod);
    }
}
