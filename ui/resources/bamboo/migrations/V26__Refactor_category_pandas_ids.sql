--
-- legacy_type
--

CREATE TABLE legacy_type (
  id TINYINT NOT NULL,
  name VARCHAR(256) NOT NULL,
  system VARCHAR(256) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO legacy_type (id, name, system)
VALUES
  (0, 'title', 'PANDAS'),
  (1, 'subject', 'PANDAS'),
  (2, 'collection', 'PANDAS'),
  (3, 'issue', 'PANDAS'),
  (4, 'issue group', 'PANDAS');

--
-- dir_category
--

ALTER TABLE dir_category ADD legacy_type_id TINYINT;
ALTER TABLE dir_category ADD legacy_id BIGINT;

ALTER TABLE dir_category ADD FOREIGN KEY (legacy_type_id) REFERENCES legacy_type (id);

CREATE UNIQUE INDEX dir_category_legacy_id ON dir_category (legacy_type_id, legacy_id);

UPDATE dir_category SET legacy_type_id = 0, legacy_id = pandas_title_id WHERE pandas_title_id IS NOT NULL;
UPDATE dir_category SET legacy_type_id = 1, legacy_id = pandas_subject_id WHERE pandas_subject_id IS NOT NULL;
UPDATE dir_category SET legacy_type_id = 2, legacy_id = pandas_collection_id WHERE pandas_collection_id IS NOT NULL;

ALTER TABLE dir_category DROP pandas_title_id;
ALTER TABLE dir_category DROP pandas_subject_id;
ALTER TABLE dir_category DROP pandas_collection_id;

--
-- dir_entry
--

ALTER TABLE dir_entry ADD legacy_type_id TINYINT;
ALTER TABLE dir_entry ADD legacy_id BIGINT;

ALTER TABLE dir_entry ADD FOREIGN KEY (legacy_type_id) REFERENCES legacy_type (id);

CREATE UNIQUE INDEX dir_entry_legacy_id ON dir_entry (legacy_type_id, legacy_id);

UPDATE dir_entry SET legacy_type_id = 0, legacy_id = pandas_title_id WHERE pandas_title_id IS NOT NULL;

ALTER TABLE dir_entry DROP pandas_title_id;

--
-- dir_agency
--

ALTER TABLE dir_agency ADD legacy_type_id TINYINT;
ALTER TABLE dir_agency ADD legacy_id BIGINT;
ALTER TABLE dir_agency ADD FOREIGN KEY (legacy_type_id) REFERENCES legacy_type (id);
