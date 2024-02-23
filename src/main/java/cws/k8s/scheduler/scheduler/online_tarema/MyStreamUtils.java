package cws.k8s.scheduler.scheduler.online_tarema;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MyStreamUtils {

    static <T extends Number> LongStream toLongStream(Stream<T> stream) {
        return stream.mapToLong(T::longValue);
    }

    static <T extends Number> DoubleStream toDoubleStream(Stream<T> stream) {
        return stream.mapToDouble(T::doubleValue);
    }

    static double floatAverage(Stream<Float> stream) {
        return toDoubleStream(stream).average().orElseThrow();
    }

    static double longAverage(Stream<Long> stream) {
        return toLongStream(stream).average().orElseThrow();
    }

    static double floatMin(Stream<Float> stream) {
        return toDoubleStream(stream).min().orElseThrow();
    }

    static long longMin(Stream<Long> stream) {
        return toLongStream(stream).min().orElseThrow();
    }

    static double floatMax(Stream<Float> stream) {
        return toDoubleStream(stream).max().orElseThrow();
    }

    static long longMax(Stream<Long> stream) {
        return toLongStream(stream).max().orElseThrow();
    }

}
