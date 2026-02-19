-- #region permissions-simple-sql
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING ((owner_id = @session.user_id) AND (done = FALSE)) WITH CHECK (owner_id = @session.user_id);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (owner_id = @session.user_id);
-- #endregion permissions-simple-sql

-- #region permissions-inherits-sql
CREATE POLICY todos_select_policy ON todos FOR SELECT USING ((done = FALSE) OR (INHERITS SELECT VIA project));
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING ((INHERITS UPDATE VIA project) AND (done = FALSE)) WITH CHECK (INHERITS UPDATE VIA project);
-- #endregion permissions-inherits-sql
