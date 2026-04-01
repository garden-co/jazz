CREATE TABLE beats (
    jamId UUID REFERENCES jams NOT NULL,
    instrumentId UUID REFERENCES instruments NOT NULL,
    beat_index INTEGER NOT NULL,
    placed_by TEXT NOT NULL
);

CREATE TABLE file_parts (
    data BYTEA NOT NULL
);

CREATE TABLE files (
    name TEXT,
    mimeType TEXT NOT NULL,
    partIds UUID[] REFERENCES file_parts NOT NULL,
    partSizes INTEGER[] NOT NULL
);

CREATE TABLE instruments (
    name TEXT NOT NULL,
    soundFileId UUID REFERENCES files NOT NULL,
    display_order INTEGER NOT NULL
);

CREATE TABLE jams (
    created_at TIMESTAMP NOT NULL,
    transport_start TIMESTAMP,
    bpm INTEGER NOT NULL,
    beat_count INTEGER NOT NULL
);

CREATE TABLE participants (
    jamId UUID REFERENCES jams NOT NULL,
    userId TEXT NOT NULL,
    display_name TEXT NOT NULL
);