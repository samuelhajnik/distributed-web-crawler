-- Per-run crawl scope: seed input, normalized seed, and strict host pair (apex + www).

ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS seed_url TEXT;
ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS normalized_seed_url TEXT;
ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS allowed_hosts TEXT[];

UPDATE crawl_runs
SET
  seed_url = root_url,
  normalized_seed_url = root_url,
  allowed_hosts = ARRAY(
    SELECT DISTINCT unnest(
      ARRAY[
        lower(split_part(split_part(root_url, '://', 2), '/', 1)),
        CASE
          WHEN lower(split_part(split_part(root_url, '://', 2), '/', 1)) LIKE 'www.%'
            THEN substring(lower(split_part(split_part(root_url, '://', 2), '/', 1)) from 5)
          ELSE 'www.' || lower(split_part(split_part(root_url, '://', 2), '/', 1))
        END
      ]
    )::text[]
  )
WHERE seed_url IS NULL OR normalized_seed_url IS NULL OR allowed_hosts IS NULL OR cardinality(allowed_hosts) = 0;

ALTER TABLE crawl_runs ALTER COLUMN seed_url SET NOT NULL;
ALTER TABLE crawl_runs ALTER COLUMN normalized_seed_url SET NOT NULL;
ALTER TABLE crawl_runs ALTER COLUMN allowed_hosts SET NOT NULL;
