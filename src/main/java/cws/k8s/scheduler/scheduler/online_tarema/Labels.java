package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@RequiredArgsConstructor
@Slf4j
public class Labels {
    final private int labelSpaceSize;
    final private int cpuLabel;
    final private int memoryLabel;
    final private int sequentialReadLabel;
    final private int sequentialWriteLabel;

    public int absoluteDifference(Labels other) throws RuntimeException {
        if (other.getLabelSpaceSize() != this.labelSpaceSize) {
            log.error("Label space size mismatch: {} != {}", other.getLabelSpaceSize(), this.labelSpaceSize);
            throw new RuntimeException("Label space size mismatch");
        }
        int cpuDifference = Math.abs(cpuLabel - other.cpuLabel);
        int memoryDifference = Math.abs(memoryLabel - other.memoryLabel);
        int sequentialReadDifference = Math.abs(sequentialReadLabel - other.sequentialReadLabel);
        int sequentialWriteDifference = Math.abs(sequentialWriteLabel - other.sequentialWriteLabel);
        return cpuDifference + memoryDifference + sequentialReadDifference + sequentialWriteDifference;
    }

}
