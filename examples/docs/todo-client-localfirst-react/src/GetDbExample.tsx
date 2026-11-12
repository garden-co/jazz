// #region writing-get-db-react
import { useDb } from "jazz-tools/react";

const db = useDb();

// Query hooks return the same handle, so a component that reads and writes needs one hook:
// const { data: todos, db } = useAll(app.todos);
// #endregion writing-get-db-react
