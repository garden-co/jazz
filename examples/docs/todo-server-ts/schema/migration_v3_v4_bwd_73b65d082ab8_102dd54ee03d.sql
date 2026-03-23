ALTER TABLE projects DROP COLUMN owner_id;
ALTER TABLE todos RENAME COLUMN project TO projectId;
ALTER TABLE todos RENAME COLUMN parent TO parentId;
ALTER TABLE todos ADD COLUMN description TEXT DEFAULT '';
