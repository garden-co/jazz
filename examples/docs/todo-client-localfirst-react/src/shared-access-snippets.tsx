import { schema as s } from "jazz-tools";
import { useAll, useDb, useSession } from "jazz-tools/react";

// #region shared-schema
const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
  todoShares: s.table({
    todoId: s.ref("todos"),
    user_id: s.uuid(),
    can_edit: s.boolean(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
// #endregion shared-schema

// #region shared-permissions
s.definePermissions(app, ({ policy, anyOf, session }) => {
  policy.todos.allowRead.where((todo) =>
    anyOf([
      { "$createdBy.account": session.user.account },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user.account,
      }),
    ]),
  );

  policy.todos.allowInsert.always();

  policy.todos.allowUpdate.where((todo) =>
    anyOf([
      { "$createdBy.account": session.user.account },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user.account,
        can_edit: true,
      }),
    ]),
  );

  policy.todos.allowDelete.where({ "$createdBy.account": session.user.account });

  // Only the todo creator can manage shares
  policy.todoShares.allowInsert.where((share) =>
    policy.todos.exists.where({
      id: share.todoId,
      "$createdBy.account": session.user.account,
    }),
  );
  policy.todoShares.allowRead.where({ user_id: session.user.account });
  policy.todoShares.allowDelete.where((share) =>
    policy.todos.exists.where({
      id: share.todoId,
      "$createdBy.account": session.user.account,
    }),
  );
});
// #endregion shared-permissions

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
    session?.user.account
      ? app.todoShares.where({ user_id: session.user.account }).include({ todo: true })
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

// #region shared-revoke
export function unshareTodo(db: ReturnType<typeof useDb>, shareId: string) {
  db.delete(app.todoShares, shareId);
}
// #endregion shared-revoke
