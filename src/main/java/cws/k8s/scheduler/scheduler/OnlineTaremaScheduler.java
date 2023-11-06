package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.scheduler.nodeassign.OnlineTaremaAssign;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OnlineTaremaScheduler extends PrioritizeAssignScheduler {

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
        log.info("Online Tarema Scheduler: Pod {} terminated.", pod);
    }

}
