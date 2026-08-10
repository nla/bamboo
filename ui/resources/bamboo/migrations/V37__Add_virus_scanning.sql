CREATE TABLE virus_scan_run (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(32) NOT NULL,
    max_warc_id BIGINT NOT NULL,
    last_warc_id BIGINT NOT NULL DEFAULT 0,
    current_warc_id BIGINT,
    clamav_version VARCHAR(255),
    warcs_total BIGINT NOT NULL DEFAULT 0,
    warcs_scanned BIGINT NOT NULL DEFAULT 0,
    records_scanned BIGINT NOT NULL DEFAULT 0,
    bytes_scanned BIGINT NOT NULL DEFAULT 0,
    findings_count BIGINT NOT NULL DEFAULT 0,
    errors_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    heartbeat_at TIMESTAMP NULL,
    error_message TEXT
);

CREATE INDEX virus_scan_run_state ON virus_scan_run (state);

CREATE TABLE virus_finding (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    warc_id BIGINT NOT NULL,
    record_offset BIGINT NOT NULL,
    warc_record_id VARCHAR(255),
    target_uri VARCHAR(4096),
    capture_time TIMESTAMP NULL,
    media_type VARCHAR(255),
    payload_digest VARCHAR(128),
    clam_signature VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    first_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    first_detected_run_id BIGINT NOT NULL,
    last_detected_run_id BIGINT NOT NULL,
    detection_count BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT fk_virus_finding_warc FOREIGN KEY (warc_id) REFERENCES warc (id),
    CONSTRAINT fk_virus_finding_first_run FOREIGN KEY (first_detected_run_id) REFERENCES virus_scan_run (id),
    CONSTRAINT fk_virus_finding_last_run FOREIGN KEY (last_detected_run_id) REFERENCES virus_scan_run (id),
    CONSTRAINT uq_virus_finding UNIQUE (warc_id, record_offset, clam_signature)
);

CREATE INDEX virus_finding_status ON virus_finding (status);
CREATE INDEX virus_finding_digest ON virus_finding (payload_digest);

CREATE TABLE virus_scan_problem (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    warc_id BIGINT NOT NULL,
    record_offset BIGINT NOT NULL DEFAULT -1,
    error_type VARCHAR(64) NOT NULL,
    message TEXT,
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_run_id BIGINT NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 1,
    CONSTRAINT fk_virus_problem_warc FOREIGN KEY (warc_id) REFERENCES warc (id),
    CONSTRAINT fk_virus_problem_run FOREIGN KEY (last_run_id) REFERENCES virus_scan_run (id),
    CONSTRAINT uq_virus_problem UNIQUE (warc_id, record_offset, error_type)
);
