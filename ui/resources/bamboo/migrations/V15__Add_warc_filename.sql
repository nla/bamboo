ALTER TABLE warc ADD filename varchar(255);
UPDATE warc SET filename = SUBSTRING_INDEX(path, '/', -1) WHERE filename IS NULL;
ALTER TABLE warc MODIFY filename varchar(255) NOT NULL;
CREATE INDEX warc_filename ON warc (filename);
