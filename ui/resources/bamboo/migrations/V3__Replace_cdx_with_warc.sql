DROP TABLE cdx_crawl;
DROP TABLE cdx;

CREATE TABLE `warc` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `crawl_id` bigint(20) NOT NULL,
  `path` varchar(4096) NOT NULL,
  `cdx_indexed` bigint(20) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_crawl_id` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
);

CREATE INDEX warc_cdx_indexed ON warc (cdx_indexed);