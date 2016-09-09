CREATE TABLE dir_agency
(
  id INT(11) NOT NULL AUTO_INCREMENT,
  name VARCHAR(256) NOT NULL,
  logo BLOB,
  url TEXT,
  PRIMARY KEY (id)
);
CREATE TABLE dir_category
(
  id BIGINT(20) NOT NULL AUTO_INCREMENT,
  parent_id BIGINT(20),
  name TEXT NOT NULL,
  description TEXT,
  pandas_subject_id INT(11),
  pandas_collection_id INT(11),
  pandas_title_id INT(11),
  PRIMARY KEY (id)
);
CREATE TABLE dir_category_agency
(
  category_id BIGINT(20) NOT NULL,
  agency_id INT(11) NOT NULL,
  PRIMARY KEY (category_id, agency_id)
);
CREATE TABLE dir_entry
(
  id BIGINT(20) NOT NULL AUTO_INCREMENT,
  category_id BIGINT(20) NOT NULL,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  snapshot_date BIGINT(20),
  pandas_title_id INT(11),
  display_order BIGINT(20),
  display_level INT(11) DEFAULT '0' NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE dir_entry_agency
(
  entry_id BIGINT(20) NOT NULL,
  agency_id INT(11) NOT NULL,
  PRIMARY KEY (entry_id, agency_id)
);
CREATE TABLE dir_symlink
(
  parent_id BIGINT(20) NOT NULL,
  target_id BIGINT(20) NOT NULL,
  PRIMARY KEY (parent_id, target_id)
);
ALTER TABLE dir_category ADD FOREIGN KEY (parent_id) REFERENCES dir_category (id);
CREATE INDEX dir_category_category_id_fk ON dir_category (parent_id);
ALTER TABLE dir_category_agency ADD FOREIGN KEY (category_id) REFERENCES dir_category (id) ON DELETE CASCADE;
ALTER TABLE dir_category_agency ADD FOREIGN KEY (agency_id) REFERENCES dir_agency (id) ON DELETE CASCADE;
CREATE INDEX dir_category_agency_dir_agency_id_fk ON dir_category_agency (agency_id);
ALTER TABLE dir_entry ADD FOREIGN KEY (category_id) REFERENCES dir_category (id);
CREATE INDEX dir_entry_category_id_fk ON dir_entry (category_id);
CREATE UNIQUE INDEX dir_entry_pandas_title_id_uindex ON dir_entry (pandas_title_id);
ALTER TABLE dir_entry_agency ADD FOREIGN KEY (entry_id) REFERENCES dir_entry (id) ON DELETE CASCADE;
ALTER TABLE dir_entry_agency ADD FOREIGN KEY (agency_id) REFERENCES dir_agency (id) ON DELETE CASCADE;
CREATE INDEX dir_entry_agency_dir_agency_id_fk ON dir_entry_agency (agency_id);
ALTER TABLE dir_symlink ADD FOREIGN KEY (parent_id) REFERENCES dir_category (id);
ALTER TABLE dir_symlink ADD FOREIGN KEY (target_id) REFERENCES dir_category (id);
CREATE INDEX dir_symlink_target_id_fk ON dir_symlink (target_id);