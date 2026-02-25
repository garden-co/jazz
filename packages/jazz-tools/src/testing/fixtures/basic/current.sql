CREATE TABLE todos (
    title TEXT NOT NULL,
    done BOOLEAN NOT NULL
);
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (done = TRUE);
