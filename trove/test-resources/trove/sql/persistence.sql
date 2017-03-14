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

CREATE TABLE `index_persistence_web_archives_restrictions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `received` datetime NOT NULL,
  `activated` datetime DEFAULT NULL,
  `retired` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  # activated but not retired will find the current ruleset
  INDEX(`activated`, `retired`),
  # received but not activated will find a new ruleset that has not been completed
  INDEX(`activated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Holds restriction ruleset metadata for the indexer for Web Archives.';

CREATE TABLE `index_persistence_web_archives_restrictions_rules` (
  `rulesSetId` int(11) NOT NULL,
  `id` int(11) NOT NULL,
  `ruleJson` TEXT NOT NULL,
  PRIMARY KEY (`rulesSetId`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Holds specific rules found inside each ruleset.';

CREATE TABLE `index_persistence_web_archives_restrictions_last_run` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `started` datetime NOT NULL,
  `dateCompleted` datetime DEFAULT NULL,
  `allCompleted` datetime DEFAULT NULL,
  `progressRuleId` int(11) DEFAULT 0,
  `workRules` int(11) DEFAULT 0,
  `workSearches` bigint(20) DEFAULT 0,
  `workDocuments` bigint(20) DEFAULT 0,
  `workWritten` bigint(20) DEFAULT 0,
  `workMsElapsed` int(11) DEFAULT 0,
  PRIMARY KEY (`id`),
  INDEX (`started`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Holds status of nightly runs to update restriction rules in the indexer.';
