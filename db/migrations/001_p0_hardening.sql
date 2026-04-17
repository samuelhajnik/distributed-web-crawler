ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS claimed_by_worker TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS http_status INTEGER;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS content_type TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS visited_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_crawl_urls_claimed_at ON crawl_urls(claimed_at);
