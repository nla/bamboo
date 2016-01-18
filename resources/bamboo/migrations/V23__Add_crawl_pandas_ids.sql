ALTER TABLE crawl ADD pandas_instance_id BIGINT;

-- allow nullable
ALTER TABLE crawl_series MODIFY path VARCHAR(4096);