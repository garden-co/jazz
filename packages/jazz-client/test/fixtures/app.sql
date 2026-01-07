-- Comprehensive test schema covering all column types, refs, and patterns
-- This schema exercises all functionality used by the demo app and more

-- Simple table with all column types
CREATE TABLE Users (
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatar STRING,
    age I64 NOT NULL,
    score F64 NOT NULL,
    isAdmin BOOL NOT NULL
);

-- Table with forward refs
CREATE TABLE Projects (
    name STRING NOT NULL,
    description STRING,
    owner REFERENCES Users NOT NULL,
    color STRING NOT NULL
);

-- Table with multiple forward refs
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

-- Simple lookup table
CREATE TABLE Tags (
    name STRING NOT NULL,
    color STRING NOT NULL
);

-- Junction table for many-to-many (Task <-> Tag)
CREATE TABLE TaskTags (
    task REFERENCES Tasks NOT NULL,
    tag REFERENCES Tags NOT NULL
);

-- Self-referential table (nested categories)
CREATE TABLE Categories (
    name STRING NOT NULL,
    parent REFERENCES Categories
);

-- Table with nullable refs
CREATE TABLE Comments (
    content STRING NOT NULL,
    author REFERENCES Users NOT NULL,
    task REFERENCES Tasks,
    parentComment REFERENCES Comments,
    createdAt I64 NOT NULL
);
