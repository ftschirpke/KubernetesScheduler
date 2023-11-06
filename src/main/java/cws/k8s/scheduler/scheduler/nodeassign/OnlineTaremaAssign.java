package cws.k8s.scheduler.scheduler.nodeassign;

import cws.k8s.scheduler.model.NodeWithAlloc;
import cws.k8s.scheduler.model.PodWithAge;
import cws.k8s.scheduler.model.Requirements;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.util.NodeTaskAlignment;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class OnlineTaremaAssign extends NodeAssign {

    @Override
    public List<NodeTaskAlignment> getTaskNodeAlignment( List<Task> unscheduledTasks, Map<NodeWithAlloc, Requirements> availableByNode ) {
        LinkedList<NodeTaskAlignment> alignments = new LinkedList<>();
        for ( Task task : unscheduledTasks ) {
            final PodWithAge pod = task.getPod();
            log.info("Tarema Assign == Pod: " + pod.getName() + " Requested Resources: " + pod.getRequest() );
            for ( Map.Entry<NodeWithAlloc, Requirements> e : availableByNode.entrySet() ) {
                if ( scheduler.canSchedulePodOnNode( e.getValue(), pod, e.getKey() ) ) {
                    alignments.add( new NodeTaskAlignment( e.getKey(), task ) );
                    log.info( "--> " + e.getKey().getName() );
                }
            }
        }
        return alignments;
    }

}
