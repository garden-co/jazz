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
    user_id: s.string(),
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
      { $createdBy: session.user_id },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user_id,
      }),
    ]),
  );

  policy.todos.allowInsert.always();

  policy.todos.allowUpdate.where((todo) =>
    anyOf([
      { $createdBy: session.user_id },
      policy.todoShares.exists.where({
        todoId: todo.id,
        user_id: session.user_id,
        can_edit: true,
      }),
    ]),
  );

  policy.todos.allowDelete.where({ $createdBy: session.user_id });

  // Only the todo creator can manage shares
  policy.todoShares.allowInsert.where((share) =>
    policy.todos.exists.where({
      id: share.todoId,
      $createdBy: session.user_id,
    }),
  );
  policy.todoShares.allowRead.where({ user_id: session.user_id });
  policy.todoShares.allowDelete.where((share) =>
    policy.todos.exists.where({
      id: share.todoId,
      $createdBy: session.user_id,
    }),
  );
});
// #endregion shared-permissions

// #region shared-grant
export function shareTodo(db: ReturnType<typeof useDb>, todoId: string, recipientUserId: string) {
  db.insert(app.todoShares, {
    todoId,
    user_id: recipientUserId,
    can_edit: false,
  });
}
// #endregion shared-grant

// #region shared-query
export function SharedWithMe() {
  const session = useSession();
  const shares = useAll(
    app.todoShares.where({ user_id: session!.user_id }).include({ todo: true }),
  );

  if (!shares) return <p>Loading…</p>;

  return (
    <ul>
      {shares.map((share) => (
        <li key={share.id}>
          {share.todo.title}
          {share.can_edit ? " (can edit)" : " (read-only)"}
        </li>
      ))}
    </ul>
  );
}
// #endregion shared-query

// #region shared-revoke
export function unshareTodo(db: ReturnType<typeof useDb>, shareId: string) {
  db.delete(app.todoShares, shareId);
}
// #endregion shared-revoke
