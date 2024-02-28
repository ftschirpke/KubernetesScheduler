package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;
import org.apache.commons.math3.ml.clustering.DoublePoint;

@Getter
public class LabelledPoint<T> extends DoublePoint {

    private final T label;

    public LabelledPoint(T label, Double point) {
        super(new double[]{point});
        this.label = label;
    }

    public LabelledPoint(T label, double[] point) {
        super(point);
        this.label = label;
    }

}
