-- Example schema for documentation
-- This schema is used to generate type-safe client code

CREATE TABLE Users (
    id STRING NOT NULL,
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatar STRING,
    age I64 NOT NULL,
    score F64 NOT NULL,
    isAdmin BOOL NOT NULL
);

CREATE TABLE Projects (
    id STRING NOT NULL,
    name STRING NOT NULL,
    description STRING,
    owner REFERENCES Users NOT NULL,
    color STRING NOT NULL
);

CREATE TABLE Tasks (
    id STRING NOT NULL,
    title STRING NOT NULL,
    description STRING,
    status STRING NOT NULL,
    priority STRING NOT NULL,
    project REFERENCES Projects NOT NULL,
    assignee REFERENCES Users,
    createdAt I64 NOT NULL,
    updatedAt I64 NOT NULL,
    isCompleted BOOL NOT NULL
);

CREATE TABLE Tags (
    id STRING NOT NULL,
    name STRING NOT NULL,
    color STRING NOT NULL
);

CREATE TABLE TaskTags (
    id STRING NOT NULL,
    task REFERENCES Tasks NOT NULL,
    tag REFERENCES Tags NOT NULL
);

CREATE TABLE Comments (
    id STRING NOT NULL,
    content STRING NOT NULL,
    author REFERENCES Users NOT NULL,
    task REFERENCES Tasks,
    parentComment REFERENCES Comments,
    createdAt I64 NOT NULL
);
