-- Test schema for @jazz/react hooks testing
-- Tests all column types and relationship patterns

CREATE TABLE Users (
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatar STRING,
    age I64 NOT NULL,
    score F64 NOT NULL,
    isAdmin BOOL NOT NULL
);

CREATE TABLE Projects (
    name STRING NOT NULL,
    description STRING,
    owner REFERENCES Users NOT NULL,
    color STRING NOT NULL
);

CREATE TABLE Tasks (
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
    name STRING NOT NULL,
    color STRING NOT NULL
);

CREATE TABLE TaskTags (
    task REFERENCES Tasks NOT NULL,
    tag REFERENCES Tags NOT NULL
);
