-- Example: old-schema clients still read priority via this backward lens.
-- Rows written by new clients are translated to priority = 0.
ALTER TABLE todos ADD COLUMN priority INTEGER DEFAULT 0;
