-- #region permissions-simple-sql
-- Users can only read their own todos
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (owner_id = @session.user_id);
-- Users cannot create todos with different owners
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (owner_id = @session.user_id);
-- Users can update their own todos, but only if not already done
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING ((owner_id = @session.user_id) AND (done = FALSE)) WITH CHECK (owner_id = @session.user_id);
-- Users can only delete their own todos
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (owner_id = @session.user_id);
-- #endregion permissions-simple-sql

-- #region permissions-always-sql
-- Allow all operations on todos (no user-scoped filtering)
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (TRUE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (TRUE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (TRUE) WITH CHECK (TRUE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (TRUE);
-- #endregion permissions-always-sql

-- #region permissions-never-sql
-- Deny all operations on todos
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (FALSE);
CREATE POLICY todos_insert_policy ON todos FOR INSERT WITH CHECK (FALSE);
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING (FALSE) WITH CHECK (FALSE);
CREATE POLICY todos_delete_policy ON todos FOR DELETE USING (FALSE);
-- #endregion permissions-never-sql

-- #region permissions-combinators-sql
-- Users can read a todo if they own it, or if it's not done and they can read its project
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (
  (owner_id = @session.user_id)
  OR ((done = FALSE) AND (INHERITS SELECT VIA project))
);
-- #endregion permissions-combinators-sql

-- #region permissions-session-claims-sql
-- Users can read a todo if they own it, or if their JWT role claim is 'manager'
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (
  (owner_id = @session.user_id)
  OR (@session.claims.role = 'manager')
);
-- #endregion permissions-session-claims-sql

-- #region permissions-inherits-sql
-- Users can read a todo if it's not done, or if they can read its project
CREATE POLICY todos_select_policy ON todos FOR SELECT USING ((done = FALSE) OR (INHERITS SELECT VIA project));
-- Users can update a todo if they can update its project and it's not done
CREATE POLICY todos_update_policy ON todos FOR UPDATE USING ((INHERITS UPDATE VIA project) AND (done = FALSE)) WITH CHECK (INHERITS UPDATE VIA project);
-- #endregion permissions-inherits-sql

-- #region permissions-shares-sql
-- Users can read a todo if they own it, or if someone shared it with them
CREATE POLICY todos_select_policy ON todos FOR SELECT USING (
  (owner_id = @session.user_id)
  OR EXISTS (
    SELECT 1 FROM todo_shares
    WHERE todo_shares.todo_id = todos.id
    AND todo_shares.user_id = @session.user_id
    AND todo_shares.can_read = TRUE
  )
);
-- #endregion permissions-shares-sql
