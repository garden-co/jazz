ALTER TABLE todos RENAME COLUMN project TO parentId;
ALTER TABLE todos RENAME COLUMN parent TO projectId;
ALTER TABLE todos RENAME COLUMN owner_id TO ownerId;
ALTER TABLE uploads RENAME COLUMN owner_id TO ownerId;
ALTER TABLE uploads RENAME COLUMN file TO fileId;
ALTER TABLE files RENAME COLUMN parts TO partIds;
