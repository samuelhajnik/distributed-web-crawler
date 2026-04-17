ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS raw_url TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS discovered_from_url_id BIGINT REFERENCES crawl_urls(id);

CREATE INDEX IF NOT EXISTS idx_crawl_urls_discovered_from ON crawl_urls(discovered_from_url_id);
