CREATE TABLE projects (
    name TEXT NOT NULL,
    owner_id TEXT NOT NULL
);

CREATE TABLE todoShares (
    todoId UUID REFERENCES todos NOT NULL,
    user_id TEXT NOT NULL,
    can_read BOOLEAN NOT NULL
);

CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL,
    parentId UUID REFERENCES todos,
    projectId UUID REFERENCES projects,
    owner_id TEXT NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (owner_id = @session.user_id) WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (owner_id = @session.user_id);