-- Linear-like issue tracker schema

CREATE TABLE Users (
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatarColor STRING NOT NULL
);

CREATE TABLE Projects (
    name STRING NOT NULL,
    color STRING NOT NULL,
    description STRING
);

CREATE TABLE Issues (
    title STRING NOT NULL,
    description STRING,
    status STRING NOT NULL,
    priority STRING NOT NULL,
    project REFERENCES Projects NOT NULL,
    createdAt I64 NOT NULL,
    updatedAt I64 NOT NULL
);

CREATE TABLE Labels (
    name STRING NOT NULL,
    color STRING NOT NULL
);

CREATE TABLE IssueLabels (
    issue REFERENCES Issues NOT NULL,
    label REFERENCES Labels NOT NULL
);

CREATE TABLE IssueAssignees (
    issue REFERENCES Issues NOT NULL,
    user REFERENCES Users NOT NULL
);
