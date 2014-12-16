package bamboo;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

import doss.*;

public class ArchiveTask {
	final Path jobPath;
	private List<Path> launches;
	private final List<Path> warcs = new ArrayList<>();
	PandasDB db;
	BlobTx blobTx;
	
	public ArchiveTask(Path jobPath) {
		this.jobPath = jobPath;
	}
	
	public void run() throws IOException {
		checkPreconditions();
	}
	
	private static boolean isLaunch(Path path) {
		return path.getFileName().toString().matches("[0-9]{14}");
	}
	
	private void checkPreconditions() throws IOException {
		Path beansFile = jobPath.resolve("crawler-beans.cxml");
		if (!Files.exists(beansFile)) {
			throw new IllegalStateException("This doesn't appear to be a valid Heritrix crawl. Missing " + beansFile);
		}
		launches = Files.list(jobPath).filter(ArchiveTask::isLaunch).collect(Collectors.toList());
		if (launches.isEmpty()) {
			throw new IllegalStateException("No launch directories found in " + jobPath);
		}
		for (Path launch: launches) {
			try {
				Files.list(launch.resolve("warcs"))
					.filter(path -> path.endsWith(".warc.gz"))
					.forEach(warcs::add);
			} catch (NoSuchFileException e) {
				// launch must have never run, skip it
			}
		}
		if (warcs.isEmpty()) {
			throw new IllegalStateException("Crawl has no WARC files: " + jobPath);
		}
		
		long crawlId = db.addCrawl();
		for (Path warc : warcs) {
			Blob blob = blobTx.put(warc);
			db.addWarc(blob.id(), warc.getFileName().toString(), warc.getParent().getFileName().toString());
		}
	}
}
