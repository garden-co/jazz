import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "./recipes/shared-access/jazz.generated.js";

// #region shared-grant
export function shareTodo(
  db: ReturnType<typeof useDb>,
  todoId: string,
  recipientAccountId: string,
) {
  db.insert(app.todoShares, {
    todoId,
    user_id: recipientAccountId,
    can_edit: false,
  });
}
// #endregion shared-grant

// #region shared-query
export function SharedWithMe() {
  const session = useSession();
  const {
    data: shares,
    isLoading,
    error,
  } = useAll(
    session?.user
      ? app.todoShares.where({ user_id: session.user }).include({ todo: true })
      : undefined,
  );

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Something went wrong!</p>;

  return (
    <ul>
      {shares?.map((share) =>
        share.todo ? (
          <li key={share.id}>
            {share.todo.title}
            {share.can_edit ? " (can edit)" : " (read-only)"}
          </li>
        ) : null,
      )}
    </ul>
  );
}
// #endregion shared-query

// #region shared-list
export function SharedWith({ todoId }: { todoId: string }) {
  const { data: shares = [], db } = useAll(app.todoShares.where({ todoId }));

  return (
    <ul>
      {shares.map((share) => (
        <li key={share.id}>
          {share.user_id}
          {share.can_edit ? " (can edit)" : " (read-only)"}
          <button onClick={() => unshareTodo(db, share.id)}>Remove</button>
        </li>
      ))}
    </ul>
  );
}
// #endregion shared-list

// #region shared-revoke
export function unshareTodo(db: ReturnType<typeof useDb>, shareId: string) {
  db.delete(app.todoShares, shareId);
}
// #endregion shared-revoke
