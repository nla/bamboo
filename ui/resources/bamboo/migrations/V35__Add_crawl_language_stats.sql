CREATE TABLE IF NOT EXISTS crawl_language_stats (
    crawl_id bigint not null,
    language varchar(32) not null,
    pages bigint not null,
    PRIMARY KEY (crawl_id, language)
);