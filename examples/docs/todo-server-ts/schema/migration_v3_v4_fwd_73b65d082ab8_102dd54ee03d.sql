ALTER TABLE todos RENAME COLUMN projectId TO project;
ALTER TABLE todos RENAME COLUMN parentId TO parent;
ALTER TABLE todos DROP COLUMN description;
ALTER TABLE projects ADD COLUMN owner_id TEXT DEFAULT '';
