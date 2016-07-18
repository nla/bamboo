ALTER TABLE crawl ADD pandas_instance_id BIGINT;
CREATE UNIQUE INDEX crawl_pandas_instance_id_uindex ON crawl (pandas_instance_id);

-- allow nullable
ALTER TABLE crawl_series MODIFY path VARCHAR(4096);