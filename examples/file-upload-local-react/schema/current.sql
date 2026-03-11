CREATE TABLE files (
    name TEXT NOT NULL,
    mimeType TEXT NOT NULL,
    parts UUID[] REFERENCES file_parts NOT NULL,
    partSizes INTEGER[] NOT NULL
);

CREATE TABLE file_parts (
    data BYTEA NOT NULL
);

CREATE TABLE uploads (
    size INTEGER NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    file_id UUID REFERENCES files NOT NULL,
    owner_id TEXT NOT NULL
);
