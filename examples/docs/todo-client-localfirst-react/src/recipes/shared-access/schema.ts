// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  todoShares: s.table(
    { todoId: s.uuid(), user_id: s.uuid(), can_edit: s.boolean() },
    {
      todo: s.rel("todos", "todoId"),
    },
  ),
  todos: s.table(
    { title: s.string(), done: s.boolean() },
    {
      shares: s.reverse("todoShares", "todo"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions, exists, session, anyOf } = schema;

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  todoShares: {
    read: (todoShare) => anyOf({ user_id: session.user }, todoShare.todo.where(createdByMe)),
    insert: (todoShare) => todoShare.todo.where(createdByMe),
    delete: (todoShare) => todoShare.todo.where(createdByMe),
  },
  todos: {
    read: (todo) =>
      anyOf(createdByMe, exists.todoShares({ todoId: todo.id, user_id: session.user })),
    insert: true,
    update: (todo) =>
      anyOf(
        createdByMe,
        exists.todoShares({ todoId: todo.id, user_id: session.user, can_edit: true }),
      ),
    delete: createdByMe,
  },
});
// #endregion permissions
