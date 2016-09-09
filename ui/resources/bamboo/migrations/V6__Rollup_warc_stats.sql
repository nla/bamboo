ALTER TABLE crawl ADD warc_files bigint(20) NOT NULL DEFAULT -1;
ALTER TABLE crawl ADD warc_size bigint(20) NOT NULL DEFAULT -1;

ALTER TABLE crawl_series ADD warc_files bigint(20) NOT NULL DEFAULT -1;
ALTER TABLE crawl_series ADD warc_size bigint(20) NOT NULL DEFAULT -1;