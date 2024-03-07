package cws.k8s.scheduler.prediction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cws.k8s.scheduler.model.SchedulerConfig;
import cws.k8s.scheduler.model.TaskMetrics;
import cws.k8s.scheduler.prediction.predictor.TestTask;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryScalerTest {

    private SchedulerConfig getSchedulerConfig( long minMemory, long maxMemory, String memoryPredictor ) {
        ObjectMapper objectMapper = new ObjectMapper();
        SchedulerConfig schedulerConfig = null;
        try {
            final String json = "{"
                    + "\"maxMemory\":" + maxMemory
                    + ",\"minMemory\":" + minMemory
                    + ",\"memoryPredictor\":\"" + memoryPredictor
                    + "\"}";
            schedulerConfig = objectMapper.readValue( json, SchedulerConfig.class);
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
        return schedulerConfig;
    }

    private TaskMetrics getTaskMetric( long runtime, long peakRss ) {
        ObjectMapper objectMapper = new ObjectMapper();
        TaskMetrics taskMetrics = null;
        try {
            final String json = "{"
                    + "\"realtime\":" + runtime
                    + ",\"peakRss\":" + peakRss
                    + "}";
            taskMetrics = objectMapper.readValue( json, TaskMetrics.class);
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
        return taskMetrics;
    }

    @Test
    public void initialTest() {
        assertThrowsExactly( IllegalArgumentException.class, () -> new MemoryScaler( getSchedulerConfig( 1024, 2048, "not there" ) ) );
        assertDoesNotThrow( () -> new MemoryScaler( getSchedulerConfig( 1024, 2048, "linear" ) ) );
    }

    @Test
    public void addTask() {

        final MemoryScaler memoryScaler = new MemoryScaler( getSchedulerConfig( 1024, 2048, "linear" ) );
        final TestTask task = new TestTask();
        task.setTaskMetrics( getTaskMetric( 1, 1 ) );
        memoryScaler.afterTaskFinished( task );

        final TestTask task2 = new TestTask();

        memoryScaler.scaleTask( task2 );

    }

}