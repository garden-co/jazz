CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    owner_id TEXT NOT NULL,
    parent UUID REFERENCES todos,
    project UUID REFERENCES projects
);
