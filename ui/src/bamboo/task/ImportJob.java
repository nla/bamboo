package bamboo.task;

import bamboo.core.*;
import bamboo.crawl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ImportJob {
	private static final Logger log = LoggerFactory.getLogger(ImportJob.class);
	private Config config;
	final Crawls crawls;
	final long crawlId;

	private HeritrixJob heritrixJob;
	private List<Path> launches;

	public ImportJob(Config config, Crawls crawls, long crawlId) {
		this.config = config;
		this.crawls = crawls;
		this.crawlId = crawlId;
	}
	
	public void run()  {
		Crawl crawl = crawls.get(crawlId);
		if (crawl.getState() != Crawl.IMPORTING) {
			return; // sanity check
		}
		if (crawl.getCrawlSeriesId() == null)
			throw new RuntimeException("TODO: implement imports without a series");

		try {
			log.info("Importing crawl id " + crawlId + " " + heritrixJob.dir());
			heritrixJob = HeritrixJob.byName(config.getHeritrixJobs(), crawl.getName());
			heritrixJob.checkSuitableForArchiving();

			Path dest = crawls.allocateCrawlPath(crawlId);
			constructCrawlBundle(heritrixJob.dir(), dest);

			crawls.addWarcs(crawlId, heritrixJob.warcs().collect(Collectors.toList()));
			crawls.updateState(crawlId, Crawl.ARCHIVED);
			log.info("Import complete of crawl id " + crawlId + " " + heritrixJob.dir());
		} catch (Exception e) {
			crawls.updateState(crawlId, Crawl.IMPORT_FAILED);
			log.error("Error importing crawl id " + crawlId + " " + heritrixJob.dir(), e);
		}
	}

	private static boolean shouldIncludeInCrawlBundle(Path path) {
		return !path.getParent().getFileName().toString().equals("warcs") &&
				Files.isRegularFile(path);
	}

	private void constructCrawlBundle(Path src, Path dest) throws IOException {
		Path zipFile = dest.resolve("crawl-bundle.zip");
		try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipFile))) {
			Files.walk(src)
					.filter(ImportJob::shouldIncludeInCrawlBundle)
					.forEachOrdered((path) -> {
						ZipEntry entry = new ZipEntry(src.relativize(path).toString());
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
}
