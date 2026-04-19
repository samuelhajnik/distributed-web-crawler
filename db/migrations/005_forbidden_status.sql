DO $$
DECLARE
  constraint_name text;
BEGIN
  FOR constraint_name IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = current_schema()
      AND rel.relname = 'crawl_urls'
      AND con.contype = 'c'
      AND pg_get_constraintdef(con.oid) ILIKE '%status%'
  LOOP
    EXECUTE format('ALTER TABLE crawl_urls DROP CONSTRAINT IF EXISTS %I', constraint_name);
  END LOOP;
END $$;

ALTER TABLE crawl_urls
  ADD CONSTRAINT crawl_urls_status_check
  CHECK (
    status IN (
      'QUEUED',
      'IN_PROGRESS',
      'VISITED',
      'REDIRECT_301',
      'FORBIDDEN',
      'NOT_FOUND',
      'HTTP_TERMINAL',
      'FAILED'
    )
  );

UPDATE crawl_urls
SET status = 'FORBIDDEN'
WHERE status = 'FAILED'
  AND http_status = 403
  AND last_error LIKE 'terminal_http_%';

UPDATE crawl_urls
SET status = 'NOT_FOUND'
WHERE status = 'FAILED'
  AND http_status = 404
  AND last_error LIKE 'terminal_http_%';

UPDATE crawl_urls
SET status = 'REDIRECT_301'
WHERE status = 'FAILED'
  AND http_status = 301
  AND (last_error LIKE 'unexpected_http_%' OR last_error LIKE 'terminal_http_%');

UPDATE crawl_urls
SET status = 'HTTP_TERMINAL'
WHERE status = 'FAILED'
  AND http_status IS NOT NULL
  AND (
    last_error LIKE 'terminal_http_%'
    OR last_error LIKE 'retryable_http_%'
    OR last_error LIKE 'unexpected_http_%'
  );
