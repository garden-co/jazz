ALTER TABLE participants RENAME COLUMN jamId TO jam;
ALTER TABLE participants RENAME COLUMN userId TO user_id;
ALTER TABLE beats RENAME COLUMN instrumentId TO jam;
ALTER TABLE beats RENAME COLUMN jamId TO instrument;
