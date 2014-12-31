package bamboo.task;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import bamboo.Config;
import bamboo.Db;
import bamboo.DbPool;

public class ImportJob implements Taskmaster.Job {
	final private DbPool dbPool;
	final Path jobPath;
	private final long crawlSeriesId;
	private List<Path> launches;
	private final List<Path> warcs = new ArrayList<>();
	Db.CrawlSeries crawlSeries;
	
	public ImportJob(DbPool dbPool, Path jobPath, long crawlSeriesId) {
		this.dbPool = dbPool;
		this.jobPath = jobPath;
		this.crawlSeriesId = crawlSeriesId;
	}
	
	public void run() throws IOException {
		checkPreconditions();
		Path destRoot = allocateTargetDir();
		Files.createDirectories(destRoot);
		copyWarcs(destRoot);
		constructCrawlBundle(destRoot);
	}

	private void copyWarcs(Path destRoot) throws IOException {
		for (int i = 0; i < warcs.size(); i++) {
			Path src = warcs.get(i);
			Path destDir = destRoot.resolve(String.format("%03d", i / 1000));
			Path dest = destDir.resolve(src.getFileName());
			if (Files.exists(dest) && Files.size(dest) == Files.size(src)) {
				continue;
			}
			if (!Files.exists(destDir)) {
				Files.createDirectory(destDir);
			}
			Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	private static boolean shouldIncludeInCrawlBundle(Path path) {
		return !path.getParent().getFileName().toString().equals("warcs") &&
				Files.isRegularFile(path);
	}

	private void constructCrawlBundle(Path destRoot) throws IOException {
		Path zipFile = destRoot.resolve("crawl-bundle.zip");
		try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipFile))) {
			Files.walk(jobPath)
					.filter(ImportJob::shouldIncludeInCrawlBundle)
					.forEachOrdered((path) -> {
						ZipEntry entry = new ZipEntry(jobPath.relativize(path).toString());
						try {
							PosixFileAttributes attr = Files.readAttributes(path, PosixFileAttributes.class);
							entry.setCreationTime(attr.creationTime());
							entry.setLastModifiedTime(attr.lastModifiedTime());
							entry.setLastAccessTime(attr.lastAccessTime());
							entry.setSize(attr.size());
							zip.putNextEntry(entry);
							Files.copy(path, zip);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					});
		}
	}

	private Path allocateTargetDir() throws IOException {
		Path destRoot;
		for (int i = 1;; i++) {
			destRoot = crawlSeries.path.resolve(String.format("%03d", i));
			try {
				Files.createDirectory(destRoot);
				break;
			} catch (FileAlreadyExistsException e) {
				// try again
			}
		}
		return destRoot;
	}

	private static boolean isLaunch(Path path) {
		return path.getFileName().toString().matches("[0-9]{14}");
	}
	
	private void checkPreconditions() throws IOException {
		Path beansFile = jobPath.resolve("crawler-beans.cxml");
		if (!Files.exists(beansFile)) {
			throw new IllegalStateException("This doesn't appear to be a valid Heritrix crawl. Missing " + beansFile);
		}
		launches = Files.list(jobPath).filter(ImportJob::isLaunch).collect(Collectors.toList());
		if (launches.isEmpty()) {
			throw new IllegalStateException("No launch directories found in " + jobPath);
		}
		for (Path launch: launches) {
			try {
				Files.list(launch.resolve("warcs"))
					.filter(path -> path.toString().endsWith(".warc.gz"))
					.forEach(warcs::add);
			} catch (NoSuchFileException e) {
				// launch must have never run, skip it
			}
		}
		if (warcs.isEmpty()) {
			throw new IllegalStateException("Crawl has no WARC files: " + jobPath);
		}
		try (Db db = dbPool.take()) {
			crawlSeries = db.findCrawlSeriesById(crawlSeriesId);
			if (crawlSeries == null) {
				throw new IllegalStateException("No such crawl series: " + crawlSeriesId);
			}
		}
	}

	@Override
	public void launch(ExecutorService executor) throws IOException {

	}

	@Override
	public String getProgress() {
		return "Archiving";
	}

}
