ALTER TABLE crawl_series ADD creator VARCHAR(255);
ALTER TABLE crawl_series ADD created TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE crawl_series ADD modifier VARCHAR(255);
ALTER TABLE crawl_series ADD modified TIMESTAMP NULL DEFAULT NULL;

ALTER TABLE crawl ADD creator VARCHAR(255);
ALTER TABLE crawl ADD created TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE crawl ADD modifier VARCHAR(255);
ALTER TABLE crawl ADD modified TIMESTAMP NULL DEFAULT NULL;