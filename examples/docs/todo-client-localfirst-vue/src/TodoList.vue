<script setup lang="ts">
import { ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app } from "../schema.js";

// #region reading-reactive-hooks-vue
const db = useDb();
const todos = useAll(app.todos);
// #endregion reading-reactive-hooks-vue

// #region reading-filtering-vue
const incompleteTodos = useAll(app.todos.where({ done: false }).orderBy("title", "asc").limit(50));
// #endregion reading-filtering-vue

// #region writing-use-db-vue
function addTodo(todoTitle: string) {
  db.insert(app.todos, { title: todoTitle, done: false });
}

function toggleTodo(todo: { id: string; done: boolean }) {
  db.update(app.todos, todo.id, { done: !todo.done });
}

function removeTodo(id: string) {
  db.delete(app.todos, id);
}
// #endregion writing-use-db-vue

// #region writing-durability-vue
async function addImportantTodo(todoTitle: string) {
  const { id } = await db.insertDurable(
    app.todos,
    { title: todoTitle, done: false },
    { tier: "edge" },
  );
  await db.updateDurable(app.todos, id, { done: true }, { tier: "edge" });
  await db.deleteDurable(app.todos, id, { tier: "global" });
}
// #endregion writing-durability-vue

const title = ref("");

function handleSubmit(event: SubmitEvent) {
  event.preventDefault();
  if (!title.value.trim()) {
    return;
  }

  addTodo(title.value.trim());
  title.value = "";
}
</script>

<template>
  <form @submit="handleSubmit">
    <input v-model="title" type="text" placeholder="What needs to be done?" required />
    <button type="submit">Add</button>
  </form>

  <ul id="todo-list">
    <li v-for="todo in todos ?? []" :key="todo.id" :class="{ done: todo.done }">
      <input type="checkbox" :checked="todo.done" class="toggle" @change="toggleTodo(todo)" />
      <span>{{ todo.title }}</span>
      <small v-if="todo.description">{{ todo.description }}</small>
      <button class="delete-btn" type="button" @click="removeTodo(todo.id)">&times;</button>
    </li>
  </ul>
</template>
