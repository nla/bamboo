alter table crawl
    add webrecorder_collection_id varchar(64) null;

create unique index crawl_webrecorder_collection_id_uindex
    on crawl (webrecorder_collection_id);