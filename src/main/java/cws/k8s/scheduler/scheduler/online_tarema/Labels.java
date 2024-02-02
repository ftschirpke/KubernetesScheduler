package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Getter
@ToString
@Slf4j
public class Labels {
    private int cpuLabel;
    private int memLabel;
    private int sequentialReadLabel;
    private int sequentialWriteLabel;

    public Labels(int cpuLabel, int memLabel, int sequentialReadLabel, int sequentialWriteLabel) {
        this.cpuLabel = cpuLabel;
        this.memLabel = memLabel;
        this.sequentialReadLabel = sequentialReadLabel;
        this.sequentialWriteLabel = sequentialWriteLabel;
    }

    public int absoluteDifference(Labels other) {
        int cpuDifference = Math.abs(cpuLabel - other.cpuLabel);
        int ramDifference = Math.abs(memLabel - other.memLabel);
        int sequentialReadDifference = Math.abs(sequentialReadLabel - other.sequentialReadLabel);
        int sequentialWriteDifference = Math.abs(sequentialWriteLabel - other.sequentialWriteLabel);
        return cpuDifference + ramDifference + sequentialReadDifference + sequentialWriteDifference;
    }

    boolean setCpuLabel(int cpuLabel) {
        if (cpuLabel == this.cpuLabel) {
            return false;
        }
        this.cpuLabel = cpuLabel;
        return true;
    }

    boolean setMemLabel(int memLabel) {
        if (memLabel == this.memLabel) {
            return false;
        }
        this.memLabel = memLabel;
        return true;
    }

    boolean setSequentialReadLabel(int sequentialReadLabel) {
        if (sequentialReadLabel == this.sequentialReadLabel) {
            return false;
        }
        this.sequentialReadLabel = sequentialReadLabel;
        return true;
    }

    boolean setSequentialWriteLabel(int sequentialWriteLabel) {
        if (sequentialWriteLabel == this.sequentialWriteLabel) {
            return false;
        }
        this.sequentialWriteLabel = sequentialWriteLabel;
        return true;
    }
}