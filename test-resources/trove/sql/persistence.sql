# This are for future reference only. Storing them alongside the classes that use this table, and as test resources
# so they never accidentally end up on a live classpath.

CREATE TABLE `index_persistance_web_archives` (
  `last_warc_id` bigint(20) NOT NULL,
  PRIMARY KEY (`last_warc_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Holds where the indexer for Web Archives is upto.';

CREATE TABLE `index_persistance_web_archives_errors` (
  `warc_id` bigint(20) NOT NULL,
  `last_error` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `retries` TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`warc_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Holds errors the indexer for Web Archives encounters.';