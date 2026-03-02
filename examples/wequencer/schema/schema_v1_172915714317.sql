CREATE TABLE beats (
    jam UUID REFERENCES jams NOT NULL,
    instrument UUID REFERENCES instruments NOT NULL,
    beat_index INTEGER NOT NULL,
    placed_by TEXT NOT NULL
);

CREATE TABLE instruments (
    name TEXT NOT NULL,
    sound BYTEA NOT NULL,
    display_order INTEGER NOT NULL
);

CREATE TABLE jams (
    created_at TIMESTAMP NOT NULL,
    transport_start TIMESTAMP,
    bpm INTEGER NOT NULL,
    beat_count INTEGER NOT NULL
);

CREATE TABLE participants (
    jam UUID REFERENCES jams NOT NULL,
    user_id TEXT NOT NULL,
    display_name TEXT NOT NULL
);