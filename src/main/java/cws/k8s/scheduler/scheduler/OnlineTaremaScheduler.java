package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RandomNodeAssign;
import cws.k8s.scheduler.scheduler.online_tarema.Labels;
import cws.k8s.scheduler.scheduler.online_tarema.NodeLabeller;
import cws.k8s.scheduler.scheduler.online_tarema.TaskLabeller;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.scheduler.prioritize.Prioritize;
import cws.k8s.scheduler.scheduler.prioritize.RankMinPrioritize;
import cws.k8s.scheduler.scheduler.trace.NextflowTraceStorage;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class OnlineTaremaScheduler extends Scheduler {

    final private Prioritize minInputPrioritize;
    final private Prioritize minRankPrioritize;
    final private NodeAssign randomNodeAssign;

    final private NextflowTraceStorage historicTraces;
    final private NodeLabeller nodeLabeller;
    final private TaskLabeller taskLabeller;

    public OnlineTaremaScheduler(String execution,
                                 KubernetesClient client,
                                 String namespace,
                                 SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.minInputPrioritize = new MinInputPrioritize();
        this.minRankPrioritize = new RankMinPrioritize();
        this.randomNodeAssign = new RandomNodeAssign();
        this.randomNodeAssign.registerScheduler(this);

        this.historicTraces = new NextflowTraceStorage();
        this.nodeLabeller = new NodeLabeller();
        this.taskLabeller = new TaskLabeller();
    }

    @Override
    void onPodTermination(PodWithAge pod) {
        super.onPodTermination(pod);
        log.info("Online Tarema Scheduler: Pod {} terminated. Saving its trace...", pod.getName());
        Task task;
        try {
            task = getTaskByPod(pod);
        } catch (IllegalStateException e) {
            log.error("Online Tarema Scheduler: Pod {} has no task associated. Skipping trace...", pod.getName());
            return;
        }
        historicTraces.saveTaskTrace(task);
        log.info("Online Tarema Scheduler: Pod {} trace saved.", pod.getName());
        // TODO: recalculate node labels for the task's node?
        recalculateTaskLabels();
    }

    @Override
    public ScheduleObject getTaskNodeAlignment(
            final List<Task> unscheduledTasks,
            final Map<NodeWithAlloc, Requirements> availableByNode
    ) {
        long start = System.currentTimeMillis();
        if (traceEnabled) {
            int index = 1;
            for (Task unscheduledTask : unscheduledTasks) {
                unscheduledTask.getTraceRecord().setSchedulerPlaceInQueue(index++);
            }
        }

        List<NodeTaskAlignment> alignment;
        if (nodeLabeller.getLabels().isEmpty()) {
            minInputPrioritize.sortTasks(unscheduledTasks);
            alignment = randomNodeAssign.getTaskNodeAlignment(unscheduledTasks, availableByNode);
        } else {
            minRankPrioritize.sortTasks(unscheduledTasks);
            alignment = align(unscheduledTasks, availableByNode);
        }

        long timeDelta = System.currentTimeMillis() - start;
        for (Task unscheduledTask : unscheduledTasks) {
            unscheduledTask.getTraceRecord().setSchedulerTimeToSchedule((int) timeDelta);
        }

        final ScheduleObject scheduleObject = new ScheduleObject(alignment);
        scheduleObject.setCheckStillPossible(false);
        return scheduleObject;
    }

    private List<NodeTaskAlignment> align(List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode) {
        ArrayList<NodeTaskAlignment> alignment = new ArrayList<>();
        for (final Task task : unscheduledTasks) {
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());
            String absoluteTaskName = task.getConfig().getTask();
            Labels taskLabels = taskLabeller.getLabels().get(absoluteTaskName);
            int triedOnNodes = 0;
            NodeWithAlloc bestNode = null;
            if (taskLabels == null) {
                // prioritize nodes with the most available resources (similar to FairAssign)
                Double highestScore = null;
                for (Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet()) {
                    if (canSchedulePodOnNode(e.getValue(), pod, e.getKey())) {
                        triedOnNodes++;
                        final double score = highestResourceAvailabilityScore(task, e.getKey(), e.getValue());
                        if (highestScore == null || score > highestScore) {
                            highestScore = score;
                            bestNode = e.getKey();
                        }
                    }
                }
            } else { // taskLabels != null
                // prioritize nodes with the least label difference (Tarema's approach)
                Integer lowestLabelDifference = null;
                for (Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet()) {
                    if (canSchedulePodOnNode(e.getValue(), pod, e.getKey())) {
                        triedOnNodes++;
                        Labels nodeLabels = nodeLabeller.getLabels().get(e.getKey().getName());
                        int labelDifference;
                        if (nodeLabels == null) {
                            labelDifference = 0; // prioritize nodes with no labels to get them labeled
                        } else {
                            labelDifference = taskLabels.absoluteDifference(nodeLabels);
                        }
                        if (lowestLabelDifference == null || labelDifference < lowestLabelDifference) {
                            lowestLabelDifference = labelDifference;
                            bestNode = e.getKey();
                        }
                    }
                }
            }
            if (bestNode != null) {
                final TraceRecord traceRecord = task.getTraceRecord();
                traceRecord.foundAlignment();
                traceRecord.setSchedulerNodesTried(triedOnNodes);
                alignment.add(new NodeTaskAlignment(bestNode, task));
                availableByNode.get(bestNode).subFromThis(pod.getRequest());
                log.info("--> " + bestNode.getName());
            }
        }
        return alignment;
    }

    private double highestResourceAvailabilityScore(Task task, NodeWithAlloc node, Requirements requirements) {
        final PodWithAge pod = task.getPod();
        final BigDecimal podCpuRequest = pod.getRequest().getCpu();
        final BigDecimal cpuMaxValue = node.getMaxResources().getCpu();
        final BigDecimal cpuMinValue = requirements.getCpu().subtract(podCpuRequest);
        final double cpuScore = cpuMinValue.doubleValue() / cpuMaxValue.doubleValue();
        final BigDecimal podRamRequest = pod.getRequest().getRam();
        final BigDecimal ramMaxValue = node.getMaxResources().getRam();
        final BigDecimal ramMinValue = requirements.getRam().subtract(podRamRequest);
        final double ramScore = ramMinValue.doubleValue() / ramMaxValue.doubleValue();
        return Double.min(cpuScore, ramScore);
    }

    void recalculateTaskLabels() {
        long startTime = System.currentTimeMillis();

        Map<String, Labels> nodesLabels = nodeLabeller.getLabels();
        if (nodesLabels.isEmpty()) {
            log.info("Online Tarema Scheduler: No node labels to calculate task labels from");
            return;
        }
        Labels maxLabels = nodeLabeller.getMaxLabels();

        float totalCpu = 0;
        float[] cpusPerCpuGroup = new float[maxLabels.getCpuLabel()];
        long totalMemory = 0;
        long[] memoryPerMemGroup = new long[maxLabels.getMemLabel()];
        int totalNodes = 0;
        int[] nodesPerSequentialReadGroup = new int[maxLabels.getSequentialReadLabel()];
        int[] nodesPerSequentialWriteGroup = new int[maxLabels.getSequentialWriteLabel()];

        for (NodeWithAlloc node : getNodeList()) {
            String nodeName = node.getName();
            if (!nodesLabels.containsKey(nodeName)) {
                continue;
            }
            Labels nodeLabels = nodesLabels.get(nodeName);

            totalCpu += node.getMaxResources().getCpu().floatValue();
            cpusPerCpuGroup[nodeLabels.getCpuLabel() - 1] += node.getMaxResources().getCpu().floatValue();
            totalMemory += node.getMaxResources().getRam().longValue();
            memoryPerMemGroup[nodeLabels.getMemLabel() - 1] += node.getMaxResources().getRam().longValue();
            totalNodes++;
            nodesPerSequentialReadGroup[nodeLabels.getSequentialReadLabel() - 1]++;
            nodesPerSequentialWriteGroup[nodeLabels.getSequentialWriteLabel() - 1]++;
        }

        float[] cpuGroupWeights = new float[maxLabels.getCpuLabel()];
        for (int i = 0; i < maxLabels.getCpuLabel(); i++) {
            cpuGroupWeights[i] = cpusPerCpuGroup[i] / totalCpu;
        }
        float[] ramGroupWeights = new float[maxLabels.getMemLabel()];
        for (int i = 0; i < maxLabels.getMemLabel(); i++) {
            ramGroupWeights[i] = (float) memoryPerMemGroup[i] / totalMemory;
        }
        float[] readGroupWeights = new float[maxLabels.getSequentialReadLabel()];
        for (int i = 0; i < maxLabels.getSequentialReadLabel(); i++) {
            readGroupWeights[i] = (float) nodesPerSequentialReadGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes reading capabilities?
        }
        float[] writeGroupWeights = new float[maxLabels.getSequentialWriteLabel()];
        for (int i = 0; i < maxLabels.getSequentialReadLabel(); i++) {
            writeGroupWeights[i] = (float) nodesPerSequentialWriteGroup[i] / totalNodes;
            // TODO: are there ways to differentiate nodes writing capabilities?
        }

        taskLabeller.recalculateLabels(historicTraces, cpuGroupWeights, ramGroupWeights, readGroupWeights, writeGroupWeights);

        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Task labels recalculated in {} ms.", endTime - startTime);
        log.info("Online Tarema Scheduler: New task labels are:\n{}", taskLabeller.getLabels());
    }

    public void recalculateNodeLabels() {
        long startTime = System.currentTimeMillis();
        boolean changed = nodeLabeller.recalculateLabels(historicTraces);
        long endTime = System.currentTimeMillis();
        log.info("Online Tarema Scheduler: Node labels recalculated in {} ms.", endTime - startTime);
        if (!changed) {
            return;
        }
        log.info("Online Tarema Scheduler: New node labels are:\n{}", nodeLabeller.getLabels());
    }
}
