CREATE TABLE users (
    name TEXT NOT NULL,
    friendsIds UUID[] REFERENCES users NOT NULL
);

CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    tags TEXT[] NOT NULL,
    projectId UUID REFERENCES projects NOT NULL,
    ownerId UUID REFERENCES users,
    assigneesIds UUID[] REFERENCES users NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (TRUE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (done = FALSE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (done = FALSE);
