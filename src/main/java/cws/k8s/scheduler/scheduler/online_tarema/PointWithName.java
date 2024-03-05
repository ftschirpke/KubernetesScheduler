package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;
import org.apache.commons.math3.ml.clustering.DoublePoint;

@Getter
public class PointWithName<T> extends DoublePoint {

    private final T name;

    public PointWithName(T name, Double point) {
        super(new double[]{point});
        this.name = name;
    }

    public PointWithName(T name, double[] point) {
        super(point);
        this.name = name;
    }

}
