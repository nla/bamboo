package bamboo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class HeritrixJob {

    private final Path dir;

    public HeritrixJob(Path dir) {

        this.dir = dir;
    }

    public FileTime getLastModified() {
        try {
            return Files.getLastModifiedTime(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getName() {
        return dir.getFileName().toString();
    }

    public static List<HeritrixJob> list(Path jobsDir) {
        try {
            return Files.list(jobsDir)
                    .filter(Files::isDirectory)
                    .map(HeritrixJob::new)
                    .sorted(Comparator.comparing(HeritrixJob::getLastModified).reversed())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static HeritrixJob byName(Path jobsDir, String jobName) {
        Path dir = jobsDir.resolve(jobName);
        if (!dir.getFileName().toString().equals(jobName)) {
            throw new IllegalArgumentException("Job name should not contain path segments: " + jobName);
        }
        if (!Files.exists(dir)) {
            throw new IllegalArgumentException("No such Heritrix Job: " + jobName);
        }
        return new HeritrixJob(dir);
    }
}
