-- Example: old-schema clients still read legacy_priority via this backward lens.
-- Rows written by new clients are translated to legacy_priority = 0.
ALTER TABLE todos ADD COLUMN legacy_priority INTEGER DEFAULT 0;
