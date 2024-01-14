package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;

import java.util.HashMap;

@Getter
public class TaskLabeller {
    private final HashMap<String, Labels> labels;

    public TaskLabeller() {
        this.labels = new HashMap<>();
    }

    public void recalculateLabels() {
        // TODO: not implemented yet
    }
}
