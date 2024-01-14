package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class NodeLabeller {
    private final HashMap<String, Labels> labels;

    public NodeLabeller() {
        // HACK: hard-coded labels for now
        this.labels = new HashMap<>(Map.of(
                "hu-worker-c29", new Labels(3, 1, 1, 1, 1),
                "hu-worker-c40", new Labels(3, 2, 2, 2, 2),
                "hu-worker-c43", new Labels(3, 3, 3, 3, 3)
        ));
    }

    public void recalculateLabels() {
        // TODO: not implemented yet
    }
}
