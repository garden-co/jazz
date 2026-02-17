-- Example: restore the dropped column with a default during rollback.
ALTER TABLE todos ADD COLUMN legacy_priority INTEGER DEFAULT 0;
