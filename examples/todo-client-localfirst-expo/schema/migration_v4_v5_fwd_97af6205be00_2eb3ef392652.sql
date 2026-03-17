ALTER TABLE todos RENAME COLUMN ownerId TO owner_id;
ALTER TABLE todos RENAME COLUMN projectId TO parent;
ALTER TABLE todos RENAME COLUMN parentId TO project;
