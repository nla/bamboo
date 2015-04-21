package bamboo.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private static boolean isLaunchPath(Path path) {
        return path.getFileName().toString().matches("[0-9]{14}");
    }

    Stream<Launch> launches() throws IOException {
        return Files.list(dir).filter(HeritrixJob::isLaunchPath).map(Launch::new);
    }

    public Path dir() {
        return dir;
    }

    public static class Launch {
        final Path dir;

        Launch(Path dir) {
            this.dir = dir;
        }

        public Stream<Path> warcs() {
            try {
                return Files.list(dir.resolve("warcs")).filter(path -> path.toString().endsWith(".warc.gz"));
            } catch (NoSuchFileException e) {
                return Stream.empty();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public Stream<Path> warcs() throws IOException {
        return launches().flatMap(Launch::warcs);
    }

    Path crawlerBeansPath() {
        return dir.resolve("crawler-beans.cxml");
    }

    public void checkSuitableForArchiving() throws IOException {
        Path beansFile = crawlerBeansPath();
        if (!Files.exists(beansFile)) {
            throw new IllegalStateException("This doesn't appear to be a valid Heritrix crawl. Missing " + beansFile);
        }
        if (!warcs().findAny().isPresent()) {
            throw new IllegalStateException("Heritrix job has no WARC files: " + dir);
        }
    }
}
