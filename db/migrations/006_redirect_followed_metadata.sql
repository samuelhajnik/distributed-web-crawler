ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS requested_url TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS final_url TEXT;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS redirected BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE crawl_urls ADD COLUMN IF NOT EXISTS final_in_scope BOOLEAN NOT NULL DEFAULT TRUE;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  SELECT conname
  INTO constraint_name
  FROM pg_constraint c
  JOIN pg_class rel ON rel.oid = c.conrelid
  WHERE rel.relname = 'crawl_urls'
    AND c.contype = 'c'
    AND pg_get_constraintdef(c.oid) LIKE '%status IN%';

  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE crawl_urls DROP CONSTRAINT IF EXISTS %I', constraint_name);
  END IF;
END $$;

ALTER TABLE crawl_urls
  ADD CONSTRAINT crawl_urls_status_check
  CHECK (
    status IN (
      'QUEUED',
      'IN_PROGRESS',
      'VISITED',
      'REDIRECT_FOLLOWED',
      'REDIRECT_OUT_OF_SCOPE',
      'REDIRECT_301',
      'FORBIDDEN',
      'NOT_FOUND',
      'HTTP_TERMINAL',
      'FAILED'
    )
  );
