CREATE TABLE projects (
    name TEXT NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    description TEXT,
    ownerId TEXT NOT NULL,
    projectId UUID REFERENCES projects,
    parentId UUID REFERENCES todos
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (ownerId = @session.user_id);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (ownerId = @session.user_id);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING ((ownerId = @session.user_id) AND (done = FALSE)) WITH CHECK (ownerId = @session.user_id);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING ((ownerId = @session.user_id) AND (done = FALSE));
