package cws.k8s.scheduler.scheduler;

import cws.k8s.scheduler.client.KubernetesClient;
import cws.k8s.scheduler.model.*;
import cws.k8s.scheduler.model.tracing.TraceRecord;
import cws.k8s.scheduler.scheduler.nodeassign.NodeAssign;
import cws.k8s.scheduler.scheduler.nodeassign.RandomNodeAssign;
import cws.k8s.scheduler.scheduler.prioritize.MinInputPrioritize;
import cws.k8s.scheduler.scheduler.prioritize.Prioritize;
import cws.k8s.scheduler.scheduler.prioritize.RankMinPrioritize;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

@Slf4j
public abstract class TaremaScheduler extends Scheduler {
    private final Prioritize minInputPrioritize = new MinInputPrioritize();
    private final Prioritize minRankPrioritize = new RankMinPrioritize();
    private final NodeAssign randomNodeAssign = new RandomNodeAssign();

    // nodes available to this execution (may not be the whole cluster)
    protected final Set<String> usedNodes = new HashSet<>();

    protected TaremaScheduler(String execution, KubernetesClient client, String namespace, SchedulerConfig config) {
        super(execution, client, namespace, config);
        this.randomNodeAssign.registerScheduler(this);
    }

    abstract int nodeTaskLabelDifference(NodeWithAlloc node, String taskName);

    abstract int nodeSpeed(NodeWithAlloc node);

    abstract boolean taskIsKnown(String taskName);

    abstract boolean nodeLabelsReady();

    @Override
    public ScheduleObject getTaskNodeAlignment(final List<Task> unscheduledTasks, final Map<NodeWithAlloc, Requirements> availableByNode) {
        long start = System.currentTimeMillis();
        if (traceEnabled) {
            int index = 1;
            for (Task unscheduledTask : unscheduledTasks) {
                unscheduledTask.getTraceRecord().setSchedulerPlaceInQueue(index++);
            }
        }

        List<NodeTaskAlignment> alignments;
        if (!nodeLabelsReady()) {
            minInputPrioritize.sortTasks(unscheduledTasks);
            alignments = randomNodeAssign.getTaskNodeAlignment(unscheduledTasks, availableByNode);
        } else {
            minRankPrioritize.sortTasks(unscheduledTasks);
            alignments = alignUsingLabels(unscheduledTasks, availableByNode);
        }

        alignments.forEach(alignment -> usedNodes.add(alignment.node.getName()));

        long timeDelta = System.currentTimeMillis() - start;
        for (Task unscheduledTask : unscheduledTasks) {
            unscheduledTask.getTraceRecord().setSchedulerTimeToSchedule((int) timeDelta);
        }

        final ScheduleObject scheduleObject = new ScheduleObject(alignments);
        scheduleObject.setCheckStillPossible(false);
        return scheduleObject;
    }

    protected List<NodeTaskAlignment> alignUsingLabels(List<Task> unscheduledTasks,
                                                       Map<NodeWithAlloc, Requirements> availableByNode) {
        ArrayList<NodeTaskAlignment> alignment = new ArrayList<>();
        for (final Task task : unscheduledTasks) {
            final PodWithAge pod = task.getPod();
            log.info("Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest());
            String abstractTaskName = task.getConfig().getTask();
            int triedOnNodes = 0;
            NodeWithAlloc bestNode = null;
            if (!taskIsKnown(abstractTaskName)) {
                // prioritize nodes with the most available resources (similar to FairAssign)
                Double highestScore = null;
                for (Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet()) {
                    NodeWithAlloc node = e.getKey();
                    Requirements requirements = e.getValue();
                    if (canSchedulePodOnNode(requirements, pod, node)) {
                        triedOnNodes++;
                        final double score = highestResourceAvailabilityScore(task, node, requirements);
                        if (highestScore == null || score > highestScore) {
                            highestScore = score;
                            bestNode = node;
                        }
                    }
                }
            } else {
                // Tarema approach: prioritize nodes with the least label difference
                // and most powerful group (or most available resources) as a tiebreaker
                Integer lowestLabelDiff = null;
                int highestSpeed = -1;
                double highestScore = 0.0;
                for (Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet()) {
                    NodeWithAlloc node = e.getKey();
                    Requirements requirements = e.getValue();
                    if (canSchedulePodOnNode(requirements, pod, node)) {
                        triedOnNodes++;
                        final int labelDifference = nodeTaskLabelDifference(node, abstractTaskName);
                        final int speed = nodeSpeed(node);
                        final double score = highestResourceAvailabilityScore(task, node, requirements);
                        if (lowestLabelDiff == null
                                || labelDifference < lowestLabelDiff
                                || (labelDifference == lowestLabelDiff && speed > highestSpeed)
                                || (labelDifference == lowestLabelDiff && speed == highestSpeed && score > highestScore)) {
                            lowestLabelDiff = labelDifference;
                            highestSpeed = speed;
                            highestScore = score;
                            bestNode = node;
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
}
