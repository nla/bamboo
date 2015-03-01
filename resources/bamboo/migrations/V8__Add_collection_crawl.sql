CREATE TABLE `collection_crawl` (
  `collection_id` bigint(20) NOT NULL,
  `crawl_id` bigint(20) NOT NULL,
  `url_filters` VARCHAR(4000),
  PRIMARY KEY (`collection_id`,`crawl_id`),
  CONSTRAINT `fk_collection_crawl_1` FOREIGN KEY (`collection_id`) REFERENCES `collection` (`id`),
  CONSTRAINT `fk_collection_crawl_2` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
);

CREATE INDEX collection_crawl_crawl_id ON collection_crawl (crawl_id);