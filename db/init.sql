CREATE TABLE IF NOT EXISTS crawl_runs (
  id BIGSERIAL PRIMARY KEY,
  root_url TEXT NOT NULL,
  seed_url TEXT NOT NULL,
  normalized_seed_url TEXT NOT NULL,
  allowed_hosts TEXT[] NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
  visited_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  duplicates_skipped INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS crawl_urls (
  id BIGSERIAL PRIMARY KEY,
  crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
  normalized_url TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('QUEUED', 'IN_PROGRESS', 'VISITED', 'FAILED')),
  claimed_at TIMESTAMPTZ,
  claimed_by_worker TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  http_status INTEGER,
  content_type TEXT,
  visited_at TIMESTAMPTZ,
  raw_url TEXT,
  discovered_from_url_id BIGINT REFERENCES crawl_urls(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (crawl_run_id, normalized_url)
);

ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS claimed_by_worker TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS http_status INTEGER;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS content_type TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS visited_at TIMESTAMPTZ;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS raw_url TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS discovered_from_url_id BIGINT REFERENCES crawl_urls(id);

CREATE INDEX IF NOT EXISTS idx_crawl_urls_run_status ON crawl_urls(crawl_run_id, status);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_updated ON crawl_urls(updated_at);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_claimed_at ON crawl_urls(claimed_at);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_discovered_from ON crawl_urls(discovered_from_url_id);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_crawl_urls_updated_at ON crawl_urls;
CREATE TRIGGER trg_crawl_urls_updated_at
BEFORE UPDATE ON crawl_urls
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at();
