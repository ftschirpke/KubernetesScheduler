package cws.k8s.scheduler.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Getter
@ToString
public class TaskConfig {

    private final String task;
    private final String name;
    private final Map< String, List<Object>> schedulerParams;
    private final TaskInput inputs;
    private final String runName;
    private final float cpus;
    private final long memoryInBytes;
    private final String workDir;

    private TaskConfig() {
        this( null );
    }

    /**
     * Only for testing
     */
    public TaskConfig(String task) {
        this(task, null);
    }

    public TaskConfig(String task, String workDir) {
        this.task = task;
        this.name = null;
        this.schedulerParams = null;
        this.inputs = new TaskInput( null, null, null, new LinkedList<>() );
        this.runName = null;
        this.cpus = 0;
        this.memoryInBytes = 0;
        this.workDir = workDir;
    }

    public TaskConfig(String task, float cpus, long memoryInBytes) {
        this.task = task;
        this.name = null;
        this.schedulerParams = null;
        this.inputs = new TaskInput( null, null, null, new LinkedList<>() );
        this.runName = null;
        this.cpus = cpus;
        this.memoryInBytes = memoryInBytes;
        this.workDir = null;
    }

    @Getter
    @ToString
    @NoArgsConstructor( access = AccessLevel.PRIVATE, force = true )
    public static class Input {

        private final String name;
        private final Object value;

    }
}
