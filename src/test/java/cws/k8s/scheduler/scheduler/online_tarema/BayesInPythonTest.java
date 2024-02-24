package cws.k8s.scheduler.scheduler.online_tarema;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.*;
import java.util.stream.Stream;

@Slf4j
public class BayesInPythonTest {

    @Test
    public void startBayesInPython() {
        log.info("Online Tarema Scheduler: Testing bayes.py from Java");
        Process bayesProcess;
        try {
            bayesProcess = new ProcessBuilder("external/venv/bin/python3", "external/bayes.py").start();
        } catch (IOException e) {
            log.error("Failed to start bayes.py process", e);
            return;
        }
        OutputStream in = bayesProcess.getOutputStream();
        PrintWriter writer = new PrintWriter(in);

        Stream<Float> cpus = Stream.of(1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f);
        cpus.forEachOrdered(f -> writer.write(f + ","));
        writer.write("cpus\n");

        Stream<Float> cpuPercentages = Stream.of(0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f);
        cpuPercentages.forEachOrdered(f -> writer.write(f + ","));
        writer.write("cpu_percentage\n");

        Stream<Long> runtimes = Stream.of(2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L);
        runtimes.forEachOrdered(f -> writer.write(f + ","));
        writer.write("runtime\n");

        writer.flush();
        writer.close();
        BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(bayesProcess.getInputStream()));
        BufferedReader stderrReader = new BufferedReader(new InputStreamReader(bayesProcess.getErrorStream()));
        String line;
        int exitCode = -1;
        try {
            exitCode = bayesProcess.waitFor();
            log.info("Online Tarema Scheduler: bayes.py exited with code {}", exitCode);
        } catch (InterruptedException e) {
            log.error("Failed to wait for bayes.py to exit", e);
        }
        try {
            while ((line = stdoutReader.readLine()) != null) {
                log.info("Online Tarema Scheduler: bayes.py stdout: {}", line);
            }
        } catch (IOException e) {
            log.error("Failed to read bayes.py stdout", e);
        }
        if (exitCode != 0) {
            log.error("Online Tarema Scheduler: bayes.py exited with code {}", exitCode);
            try {
                while ((line = stderrReader.readLine()) != null) {
                    log.error("Online Tarema Scheduler: bayes.py stderr: {}", line);
                }
            } catch (IOException e2) {
                log.error("Failed to read bayes.py stderr", e2);
            }
        }
    }
}
