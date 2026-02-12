CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    parent UUID REFERENCES todos
);