const list = document.getElementById("todos");

db.subscribeAll(app.todos, ({ all: todos }) => {
  list.replaceChildren(
    ...todos.map((todo) => {
      const li = Object.assign(document.createElement("li"), {
        textContent: todo.title,
        onclick: () => db.update(app.todos, todo.id, { done: !todo.done }),
      });
      if (todo.done) li.style.textDecoration = "line-through";
      return li;
    }),
  );
});
