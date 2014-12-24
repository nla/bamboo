package bamboo.task;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.READ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

public class Taskmaster {
    private ExecutorService threadPool = Executors.newWorkStealingPool();
    private List<Job> jobs = new ArrayList<Job>();

    public void launch(Job job) throws IOException {
        jobs.add(job);
        job.launch(threadPool);
    }

    public static interface Job {
        void launch(ExecutorService executor) throws IOException;
        String getProgress();
    }

    public List<Job> getJobs() {
        return Collections.unmodifiableList(jobs);
    }
}
