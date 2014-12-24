CREATE TABLE `collection` (
  `id` bigint(20) NOT NULL,
  `name` varchar(4096) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `cdx` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `collection_id` bigint(20) NOT NULL,
  `path` varchar(4096) NOT NULL,
  `total_docs` bigint(20) DEFAULT NULL,
  `total_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_collection_id` FOREIGN KEY (`collection_id`) REFERENCES `collection` (`id`)
);

CREATE TABLE `crawl_series` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(4096) NOT NULL,
  `path` varchar(4096) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `crawl` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(4096) NOT NULL,
  `total_docs` bigint(20) DEFAULT NULL,
  `total_bytes` bigint(20) DEFAULT NULL,
  `crawl_series_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_crawl_1` FOREIGN KEY (`crawl_series_id`) REFERENCES `crawl_series` (`id`)
);

CREATE TABLE `cdx_crawl` (
  `cdx_id` bigint(20) NOT NULL,
  `crawl_id` bigint(20) NOT NULL,
  PRIMARY KEY (`cdx_id`,`crawl_id`),
  CONSTRAINT `fk_cdx_crawl_1` FOREIGN KEY (`cdx_id`) REFERENCES `cdx` (`id`),
  CONSTRAINT `fk_cdx_crawl_2` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
);

CREATE TABLE `import_task` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `job_path` varchar(4096) NOT NULL,
  `crawl_series_id` bigint(20) DEFAULT NULL,
  `state` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_import_task_crawl_series_id` FOREIGN KEY (`crawl_series_id`) REFERENCES `crawl_series` (`id`)
);

CREATE INDEX import_task_state ON import_task (state);