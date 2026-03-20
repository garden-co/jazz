ALTER TABLE files RENAME COLUMN partIds TO parts;
ALTER TABLE uploads RENAME COLUMN ownerId TO owner_id;
ALTER TABLE uploads RENAME COLUMN fileId TO file;
ALTER TABLE todos RENAME COLUMN parentId TO project;
ALTER TABLE todos RENAME COLUMN projectId TO parent;
ALTER TABLE todos RENAME COLUMN ownerId TO owner_id;
