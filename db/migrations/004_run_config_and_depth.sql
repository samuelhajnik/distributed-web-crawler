ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS run_config JSONB;
UPDATE crawl_runs SET run_config = '{}'::jsonb WHERE run_config IS NULL;
ALTER TABLE crawl_runs ALTER COLUMN run_config SET NOT NULL;
ALTER TABLE crawl_runs ALTER COLUMN run_config SET DEFAULT '{}'::jsonb;

ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS depth INTEGER;
UPDATE crawl_urls SET depth = 0 WHERE depth IS NULL;
ALTER TABLE crawl_urls ALTER COLUMN depth SET NOT NULL;
ALTER TABLE crawl_urls ALTER COLUMN depth SET DEFAULT 0;

