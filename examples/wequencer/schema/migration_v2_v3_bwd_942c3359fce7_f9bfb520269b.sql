ALTER TABLE instruments ADD COLUMN sound BYTEA DEFAULT '\\x';
ALTER TABLE instruments DROP COLUMN soundFileId;


