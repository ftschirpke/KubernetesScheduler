package cws.k8s.scheduler.scheduler.online_tarema;

public record Labels(int cpuLabel, int memLabel, int sequentialReadLabel, int sequentialWriteLabel) {

    public String toString() {
        return "Labels(cpu=" + cpuLabel + ", mem=" + memLabel + ", read=" + sequentialReadLabel + ", write=" + sequentialWriteLabel + ")";
    }

    public int absoluteDifference(Labels other) {
        int cpuDifference = Math.abs(cpuLabel - other.cpuLabel);
        int ramDifference = Math.abs(memLabel - other.memLabel);
        int sequentialReadDifference = Math.abs(sequentialReadLabel - other.sequentialReadLabel);
        int sequentialWriteDifference = Math.abs(sequentialWriteLabel - other.sequentialWriteLabel);
        return cpuDifference + ramDifference + sequentialReadDifference + sequentialWriteDifference;
    }
}