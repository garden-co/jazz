ALTER TABLE beats RENAME COLUMN jam TO instrumentId;
ALTER TABLE beats RENAME COLUMN instrument TO jamId;
ALTER TABLE participants RENAME COLUMN jam TO jamId;
ALTER TABLE participants RENAME COLUMN user_id TO userId;
