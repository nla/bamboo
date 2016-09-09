CREATE TABLE seedlist (
    id bigint(20) NOT NULL AUTO_INCREMENT,
    name varchar(4096) NOT NULL,
    total_seeds bigint(20) NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE TABLE seed (
    id bigint(20) NOT NULL AUTO_INCREMENT,
    url varchar(4096) NOT NULL,
    surt varchar(4096) NOT NULL,
    seedlist_id bigint(20) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_seed_list_id FOREIGN KEY (seedlist_id) REFERENCES seedlist (id)
);

CREATE INDEX seedlist_url ON seed (seedlist_id, url);
CREATE INDEX seedlist_surt ON seed (seedlist_id, surt);