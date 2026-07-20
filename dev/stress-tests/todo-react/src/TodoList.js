import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { app } from "../schema";
export function TodoList() {
  const [filterTitle, setFilterTitle] = useState("");
  const [showDoneOnly, setShowDoneOnly] = useState(false);
  const trimmedFilterTitle = filterTitle.trim();
  let todosQuery = app.todos.limit(100);
  if (trimmedFilterTitle) {
    todosQuery = todosQuery.where({ title: { contains: trimmedFilterTitle } });
  }
  if (showDoneOnly) {
    todosQuery = todosQuery.where({ done: true });
  }
  const db = useDb();
  // #region reading-reactive-hooks-react
  const { data: todos = [] } = useAll(todosQuery);
  // #endregion reading-reactive-hooks-react
  const session = useSession();
  const sessionUserId = session?.user_id ?? null;
  const [title, setTitle] = useState("");
  const handleSubmit = (e) => {
    e.preventDefault();
    if (!title.trim() || !sessionUserId) return;
    db.insert(app.todos, {
      title: title.trim(),
      done: false,
      owner_id: sessionUserId,
    });
    setTitle("");
  };
  return _jsxs(_Fragment, {
    children: [
      _jsxs("form", {
        onSubmit: handleSubmit,
        children: [
          _jsx("input", {
            type: "text",
            value: title,
            onChange: (e) => setTitle(e.target.value),
            placeholder: "What needs to be done?",
            required: true,
          }),
          _jsx("button", { type: "submit", disabled: !sessionUserId, children: "Add" }),
        ],
      }),
      _jsxs("div", {
        children: [
          _jsx("input", {
            type: "text",
            value: filterTitle,
            onChange: (e) => setFilterTitle(e.target.value),
            placeholder: "Filter by title (contains)",
            "aria-label": "Filter by title",
          }),
          _jsxs("label", {
            children: [
              _jsx("input", {
                type: "checkbox",
                checked: showDoneOnly,
                onChange: (e) => setShowDoneOnly(e.target.checked),
              }),
              "Done only",
            ],
          }),
        ],
      }),
      _jsx("ul", {
        id: "todo-list",
        children: todos.map((todo) =>
          _jsxs(
            "li",
            {
              className: todo.done ? "done" : "",
              children: [
                _jsx("input", {
                  type: "checkbox",
                  checked: todo.done,
                  onChange: () => db.update(app.todos, todo.id, { done: !todo.done }),
                  className: "toggle",
                }),
                _jsx("span", { children: todo.title }),
                todo.description && _jsx("small", { children: todo.description }),
                _jsx("button", {
                  className: "delete-btn",
                  onClick: () => db.delete(app.todos, todo.id),
                  children: "\u00D7",
                }),
              ],
            },
            todo.id,
          ),
        ),
      }),
    ],
  });
}
