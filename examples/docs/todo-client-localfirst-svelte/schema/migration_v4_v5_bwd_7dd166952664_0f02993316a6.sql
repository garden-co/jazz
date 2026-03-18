ALTER TABLE todos RENAME COLUMN project TO parentId;
ALTER TABLE todos RENAME COLUMN parent TO projectId;
