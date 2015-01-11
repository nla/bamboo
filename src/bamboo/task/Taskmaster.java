package bamboo.task;

import java.io.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Taskmaster {
    private ExecutorService threadPool = Executors.newWorkStealingPool();
    private List<Job> jobs = new ArrayList<>();

    public Future<?> launch(Job job) throws IOException {
        return threadPool.submit(() -> {
            jobs.add(job);
            ProgressMonitor monitor = new ProgressMonitor();
            try {
                job.run(monitor);
            } catch (Throwable t) {
                monitor.error(t);
            } finally {
                monitor.finish();
                jobs.remove(job);
            }
        });
    }

    public static interface Job {
        void run(IProgressMonitor progress) throws IOException;
    }

    public static interface IProgressMonitor {
        void begin(String name);
        void progress(int completeness);
        void finish();
        void error(Throwable t);
    }

    public static class ProgressMonitor implements IProgressMonitor {
        String name = "Unknown job";

        @Override
        public void begin(String name) {
            this.name = name;
            System.out.println(name + ": started.");
        }

        @Override
        public void progress(int completeness) {
            System.out.println(name + ": " + completeness);
        }

        @Override
        public void finish() {
            System.out.println(name + ": finished.");
        }

        @Override
        public void error(Throwable t) {
            System.out.println(name + ": error.");
            t.printStackTrace();
        }
    }

    public List<Job> getJobs() {
        return Collections.unmodifiableList(jobs);
    }
}
