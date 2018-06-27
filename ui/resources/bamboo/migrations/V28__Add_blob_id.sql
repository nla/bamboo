ALTER TABLE warc ADD blob_id bigint;
ALTER TABLE artifact ADD (blob_id bigint, relpath VARCHAR(255), last_modified TIMESTAMP);

-- make path fields nullable
ALTER TABLE warc MODIFY path varchar(4096) NULL;
ALTER TABLE artifact MODIFY path varchar(255) NULL;
