ALTER TABLE crawl ADD description TEXT;
ALTER TABLE crawl_series ADD description TEXT;
ALTER TABLE collection ADD description TEXT;

ALTER TABLE collection_series DROP FOREIGN KEY fk_collection_series_1;
ALTER TABLE collection MODIFY COLUMN id BIGINT AUTO_INCREMENT;
ALTER TABLE collection_series ADD CONSTRAINT fk_collection_series_1 FOREIGN KEY (collection_id) REFERENCES collection (id);
