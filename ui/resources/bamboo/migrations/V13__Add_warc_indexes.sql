CREATE INDEX warc_solr_indexed ON warc (solr_indexed);
CREATE INDEX warc_crawl_cdx_indexed ON warc (crawl_id, cdx_indexed, corrupt);
CREATE INDEX warc_crawl_solr_indexed ON warc (crawl_id, solr_indexed, corrupt);
CREATE INDEX warc_crawl_corrupt ON warc (crawl_id, corrupt);