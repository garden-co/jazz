CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    parentId UUID REFERENCES todos,
    projectId UUID REFERENCES projects
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (done = FALSE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (done = FALSE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (done = FALSE);
