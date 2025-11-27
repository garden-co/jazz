import { co, z } from "jazz-tools";

export const TodoItem = co.map({
  title: z.string(),
  completed: z.boolean(),
});

export const AccountRoot = co.map({
  todos: co.list(TodoItem),
});

export const MyAppAccount = co.account({
  root: AccountRoot,
  profile: co.profile(),
});
