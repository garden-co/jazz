// #region prefetch
import { app } from "../schema";
import { db, createServerSnapshot } from "@/lib/jazz-server";
import TodoPanel from "./TodoPanel";

// This prefetches with the full-access `db` because the todos are shared. For
// per-user data, prefetch with `dbForSession(session)` instead so the snapshot
// only carries rows that viewer is allowed to read — never serialise private
// data into the page's HTML.
export default async function PrefetchedTodoPanel() {
  const builder = createServerSnapshot();
  await builder.prefetch(db, app.todos);
  const snapshot = builder.dehydrate();

  return <TodoPanel snapshot={snapshot} />;
}
// #endregion prefetch
