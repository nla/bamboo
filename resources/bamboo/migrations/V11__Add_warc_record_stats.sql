ALTER TABLE warc ADD records bigint(20) NOT NULL DEFAULT 0;
ALTER TABLE warc ADD record_bytes bigint(20) NOT NULL DEFAULT 0;

ALTER TABLE crawl ADD records bigint(20) NOT NULL DEFAULT 0;
ALTER TABLE crawl ADD record_bytes bigint(20) NOT NULL DEFAULT 0;

ALTER TABLE crawl_series ADD records bigint(20) NOT NULL DEFAULT 0;
ALTER TABLE crawl_series ADD record_bytes bigint(20) NOT NULL DEFAULT 0;

ALTER TABLE collection ADD records bigint(20) NOT NULL DEFAULT 0;
ALTER TABLE collection ADD record_bytes bigint(20) NOT NULL DEFAULT 0;