CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    tags TEXT[] NOT NULL,
    project UUID REFERENCES projects NOT NULL
);
