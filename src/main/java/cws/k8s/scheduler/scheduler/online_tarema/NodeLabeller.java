package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class NodeLabeller {
    private final int labelSpaceSize;
    private final HashMap<String, Labels> labels;

    public NodeLabeller(int labelSpaceSize) {
        this.labelSpaceSize = labelSpaceSize;
        // HACK: hard-coded labels for now
        this.labels = new HashMap<>(Map.of(
                "hu-worker-c29", new Labels(labelSpaceSize, 1, 1, 1, 1),
                "hu-worker-c40", new Labels(labelSpaceSize, 2, 2, 2, 2),
                "hu-worker-c43", new Labels(labelSpaceSize, 3, 3, 3, 3)
        ));
    }

    public void recalculateLabels() {
        // TODO: not implemented yet
    }
}
