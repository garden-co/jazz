CREATE TABLE file_parts (
    data BYTEA NOT NULL
);

CREATE TABLE files (
    name TEXT NOT NULL,
    mimeType TEXT NOT NULL,
    partIds UUID[] REFERENCES file_parts NOT NULL,
    partSizes INTEGER[] NOT NULL
);

CREATE TABLE uploads (
    size INTEGER NOT NULL,
    lastModified TIMESTAMP NOT NULL,
    fileId UUID REFERENCES files NOT NULL,
    ownerId TEXT NOT NULL
);