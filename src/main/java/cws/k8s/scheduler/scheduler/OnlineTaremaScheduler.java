package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nodeassign.OnlineTaremaAssign;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OnlineTaremaScheduler extends PrioritizeAssignScheduler {

    final private int labelSpaceSize;
    final private NextflowTraceStorage historicTraces;

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config,
                                 int labelSpaceSize) {
        super(execution, client, namespace, config, new MinInputPrioritize(), new OnlineTaremaAssign());
        this.labelSpaceSize = labelSpaceSize;
        this.historicTraces = new NextflowTraceStorage();
    }

    @Override
    void podEventReceived(Watcher.Action action, Pod pod) {
        log.info("Online Tarema Scheduler: Received Event {} for pod {}.", action, pod);
    }

    @Override
    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);
        log.info("Online Tarema Scheduler: Pod {} terminated. Saving its trace...", pod);
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Online Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod);
            return;
        }
        historicTraces.saveTaskTrace(task);
    }
}