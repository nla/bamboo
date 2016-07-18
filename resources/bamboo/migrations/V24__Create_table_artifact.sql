CREATE TABLE artifact (
  id BIGINT NOT NULL AUTO_INCREMENT,
  crawl_id BIGINT NOT NULL,
  type VARCHAR(128) NOT NULL,
  path VARCHAR(255) NOT NULL UNIQUE,
  size BIGINT NOT NULL,
  sha256 VARCHAR(128) NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_artifact_crawl_id FOREIGN KEY (crawl_id) REFERENCES crawl (id)
);

CREATE INDEX artifact_crawl_id_type ON artifact (crawl_id, type);