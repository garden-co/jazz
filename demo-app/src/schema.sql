-- Demo app schema with pluralized table names

CREATE TABLE Users (
    name STRING NOT NULL,
    email STRING NOT NULL,
    avatar STRING
);

CREATE TABLE Folders (
    name STRING NOT NULL,
    owner REFERENCES Users NOT NULL,
    parent REFERENCES Folders
);

CREATE TABLE Notes (
    title STRING NOT NULL,
    content STRING NOT NULL,
    author REFERENCES Users NOT NULL,
    folder REFERENCES Folders,
    createdAt I64 NOT NULL,
    updatedAt I64 NOT NULL
);

CREATE TABLE Tags (
    name STRING NOT NULL,
    color STRING NOT NULL
);
