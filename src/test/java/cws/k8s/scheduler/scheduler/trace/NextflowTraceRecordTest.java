package cws.k8s.scheduler.scheduler.trace;

import cws.k8s.scheduler.dag.DAG;
import cws.k8s.scheduler.dag.InputEdge;
import cws.k8s.scheduler.dag.Process;
import cws.k8s.scheduler.dag.Vertex;
import cws.k8s.scheduler.model.Task;
import cws.k8s.scheduler.model.TaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class NextflowTraceRecordTest {

    @Test
    public void readExampleTrace() {
        final DAG dag = new DAG();
        final Process a = new Process("a", 1);
        List<Vertex> vertexList = List.of(a);
        List<InputEdge> inputEdges = new LinkedList<>();
        dag.registerVertices(vertexList);
        dag.registerEdges(inputEdges);

        Path path = Paths.get("test-files");
        System.out.println(path.toAbsolutePath());
        final TaskConfig config = new TaskConfig("a", path.toString());
        final Task task = new Task(config, dag);

        final NextflowTraceRecord traceRecord = NextflowTraceRecord.from_task(task);

        assertEquals(15L,traceRecord.getTimeValue("realtime"));
        assertEquals(790.0f, traceRecord.getPercentageValue("%cpu"));
        assertEquals("Some CPU @ 3GHz",traceRecord.getStringValue("cpu_model"));
        assertEquals(62098L, traceRecord.getMemoryValue("rchar"));
        assertEquals(225L, traceRecord.getMemoryValue("wchar"));
        assertEquals(147, traceRecord.getIntegerValue("syscr"));
        assertEquals(13, traceRecord.getIntegerValue("syscw"));
        assertEquals(0L, traceRecord.getMemoryValue("read_bytes"));
        assertEquals(0L, traceRecord.getMemoryValue("write_bytes"));
        assertEquals(0.0f, traceRecord.getPercentageValue("%mem"));
        assertEquals(4360L, traceRecord.getMemoryValue("vmem"));
        assertEquals(1488L, traceRecord.getMemoryValue("rss"));
        assertEquals(4360L, traceRecord.getMemoryValue("peak_vmem"));
        assertEquals(1488L, traceRecord.getMemoryValue("peak_rss"));
        assertEquals(3, traceRecord.getIntegerValue("vol_ctxt"));
        assertEquals(0, traceRecord.getIntegerValue("inv_ctxt"));
    }

}
