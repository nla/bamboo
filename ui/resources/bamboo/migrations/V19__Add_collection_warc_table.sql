CREATE TABLE collection_warc (
    collection_id bigint NOT NULL,
    warc_id bigint NOT NULL,
    records bigint NOT NULL DEFAULT 0,
    record_bytes bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (collection_id, warc_id),
    CONSTRAINT fk_collection_warc_1 FOREIGN KEY (collection_id) REFERENCES collection (id),
    CONSTRAINT fk_collection_warc_2 FOREIGN KEY (warc_id) REFERENCES warc (id)
);