CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    owner_id TEXT,
    priority INTEGER,
    created_at TIMESTAMP,
    parent UUID REFERENCES todos,
    project UUID REFERENCES projects
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (TRUE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (TRUE);

CREATE TABLE file_parts (
    data BYTEA NOT NULL
);

CREATE TABLE files (
    name TEXT,
    mimeType TEXT NOT NULL,
    parts UUID[] REFERENCES file_parts NOT NULL,
    partSizes INTEGER[] NOT NULL
);

CREATE TABLE uploads (
    owner_id TEXT NOT NULL,
    label TEXT NOT NULL,
    file UUID REFERENCES files NOT NULL
);
