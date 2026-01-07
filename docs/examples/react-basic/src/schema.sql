-- Simple task management schema for documentation examples

CREATE TABLE Tasks (
    title STRING NOT NULL,
    description STRING,
    completed BOOLEAN NOT NULL,
    priority STRING NOT NULL,
    createdAt I64 NOT NULL
);
