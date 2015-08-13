CREATE TABLE warc_state (
    id INTEGER NOT NULL,
    name VARCHAR(72) NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX warc_state_name ON warc_state (name);

INSERT INTO warc_state (id, name)
VALUES (0,  'open'),
       (1,  'imported'),
	   (2,  'cdx_indexed'),
       (3,  'solr_indexed'),
       (-1, 'import_error'),
       (-2, 'cdx_error'),
       (-3, 'solr_error');

CREATE TABLE warc_history (
    id BIGINT NOT NULL AUTO_INCREMENT,
    warc_id BIGINT NOT NULL,
    time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    warc_state_id INTEGER NOT NULL,
	PRIMARY KEY (id),
    CONSTRAINT fk_warc_history_warc_id FOREIGN KEY (warc_id) REFERENCES warc (id),
	CONSTRAINT fk_warc_history_warc_state_id FOREIGN KEY (warc_state_id) REFERENCES warc_state (id)
);

ALTER TABLE warc ADD warc_state_id INTEGER NOT NULL DEFAULT 0 AFTER crawl_id;
ALTER TABLE warc ADD CONSTRAINT fk_warc_warc_state_id FOREIGN KEY (warc_state_id) REFERENCES warc_state (id);
CREATE INDEX warc_crawl_id_warc_state_id ON warc (crawl_id, warc_state_id);
CREATE INDEX warc_warc_state_id ON warc (warc_state_id);

UPDATE warc SET warc_state_id =
    (CASE WHEN path LIKE '%.open' THEN (SELECT id FROM warc_state WHERE name = 'open')
		  WHEN corrupt > 0 THEN (SELECT id FROM warc_state WHERE name = 'cdx_error')
          WHEN cdx_indexed > 0 AND solr_indexed > 0 THEN (SELECT id FROM warc_state WHERE name = 'solr_indexed')
          WHEN cdx_indexed > 0 THEN (SELECT id FROM warc_state WHERE name = 'cdx_indexed')
          ELSE (SELECT id FROM warc_state WHERE name = 'imported')
          END) WHERE id >= 0;

INSERT INTO warc_history (warc_id, time, warc_state_id)
SELECT
	id AS warc_id,
    FROM_UNIXTIME(cdx_indexed / 1000) AS time,
    (SELECT id FROM warc_state WHERE name = 'cdx_indexed') AS warc_state_id
FROM warc
WHERE cdx_indexed > 0;

INSERT INTO warc_history (warc_id, time, warc_state_id)
SELECT id AS warc_id,
	   FROM_UNIXTIME(solr_indexed / 1000) AS time,
       (SELECT id FROM warc_state WHERE name = 'solr_indexed') AS warc_state_id
FROM warc
WHERE solr_indexed > 0;

ALTER TABLE warc DROP INDEX warc_crawl_corrupt;
ALTER TABLE warc DROP INDEX warc_crawl_cdx_indexed;
ALTER TABLE warc DROP INDEX warc_crawl_solr_indexed;

ALTER TABLE warc DROP cdx_indexed;
ALTER TABLE warc DROP solr_indexed;
ALTER TABLE warc DROP corrupt;
