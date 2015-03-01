CREATE TABLE `collection_series` (
  `collection_id` bigint(20) NOT NULL,
  `crawl_series_id` bigint(20) NOT NULL,
  `url_filters` VARCHAR(4000),
  PRIMARY KEY (`collection_id`,`crawl_series_id`),
  CONSTRAINT `fk_collection_series_1` FOREIGN KEY (`collection_id`) REFERENCES `collection` (`id`),
  CONSTRAINT `fk_collection_series_2` FOREIGN KEY (`crawl_series_id`) REFERENCES `crawl_series` (`id`)
);

CREATE INDEX collection_series_crawl_series_id ON collection_series (crawl_series_id);