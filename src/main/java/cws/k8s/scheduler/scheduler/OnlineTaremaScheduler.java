package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.scheduler.nodeassign.OnlineTaremaAssign;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class OnlineTaremaScheduler extends PrioritizeAssignScheduler {

    final private int labelSpaceSize;
    final private NextflowTraceStorage historicTraces;
    final private NodeLabeller nodeLabeller;
    final private TaskLabeller taskLabeller;

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config,
                                 int labelSpaceSize) {
        super(execution, client, namespace, config, new MinInputPrioritize(), new OnlineTaremaAssign());
        this.labelSpaceSize = labelSpaceSize;
        this.historicTraces = new NextflowTraceStorage();
        this.nodeLabeller = new NodeLabeller(labelSpaceSize);
        this.taskLabeller = new TaskLabeller(labelSpaceSize);
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        float totalCpu = 0;
        float[] cpusPerCpuGroup = new float[labelSpaceSize];
        long totalMemory = 0;
        long[] memoryPerRamGroup = new long[labelSpaceSize];
        // int[] rcharPerReadGroup = new int[labelSpaceSize]; // TODO: just ideas, but what should this really be?
        // int[] wcharPerWriteGroup = new int[labelSpaceSize];
        Map<String, Labels> nodesLabels = nodeLabeller.getLabels();
        for (NodeWithAlloc node : getNodeList()) {
            String nodeName = node.getName();
            if (!nodesLabels.containsKey(nodeName)) {
                continue;
            }
            Labels nodeLabels = nodesLabels.get(nodeName);
            totalCpu += node.getMaxResources().getCpu().floatValue();
            cpusPerCpuGroup[nodeLabels.getCpuLabel() - 1] += node.getMaxResources().getCpu().floatValue();
            totalMemory += node.getMaxResources().getRam().longValue();
            memoryPerRamGroup[nodeLabels.getRamLabel() - 1] += node.getMaxResources().getRam().longValue();
            // TODO: sequential read and write
        }
        float[] cpuGroupWeights = new float[labelSpaceSize];
        for (int i = 0; i < labelSpaceSize; i++) {
            cpuGroupWeights[i] = cpusPerCpuGroup[i] / totalCpu;
        }
        float[] ramGroupWeights = new float[labelSpaceSize];
        for (int i = 0; i < labelSpaceSize; i++) {
            ramGroupWeights[i] = (float) memoryPerRamGroup[i] / totalMemory;
        }
        float[] readGroupWeights = new float[labelSpaceSize];
        for (int i = 0; i < labelSpaceSize; i++) {
            readGroupWeights[i] = (float) 1 / labelSpaceSize; // TODO: sequential read
        }
        float[] writeGroupWeights = new float[labelSpaceSize];
        for (int i = 0; i < labelSpaceSize; i++) {
            writeGroupWeights[i] = (float) 1 / labelSpaceSize; // TODO: sequential write
        }
        taskLabeller.recalculateLabels(historicTraces, cpuGroupWeights, ramGroupWeights, readGroupWeights, writeGroupWeights);

        long endTime = System.currentTimeMillis();
        log.debug("Online Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
        log.info("Online Tarema Scheduler: New task labels are:\n{}", taskLabeller.getLabels());
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
        log.info("Online Tarema Scheduler: Pod {} trace saved.", pod);
        recalculateTaskLabels();
    }
}