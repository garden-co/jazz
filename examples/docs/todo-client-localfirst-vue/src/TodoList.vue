<script setup lang="ts">
import { ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app } from "../schema/app.js";

// #region reading-reactive-hooks-vue
const db = useDb();
const todos = useAll(app.todos);
// #endregion reading-reactive-hooks-vue

// #region reading-filtering-vue
const incompleteTodos = useAll(app.todos.where({ done: false }).orderBy("title", "asc").limit(50));
// #endregion reading-filtering-vue

// #region writing-use-db-vue
async function addTodo(todoTitle: string) {
  await db.insert(app.todos, { title: todoTitle, done: false });
}

async function toggleTodo(todo: { id: string; done: boolean }) {
  await db.update(app.todos, todo.id, { done: !todo.done });
}

async function removeTodo(id: string) {
  await db.deleteFrom(app.todos, id);
}
// #endregion writing-use-db-vue

// #region writing-durability-vue
async function addImportantTodo(todoTitle: string) {
  const id = await db.insert(app.todos, { title: todoTitle, done: false }, { tier: "edge" });
  await db.update(app.todos, id, { done: true }, { tier: "edge" });
  await db.deleteFrom(app.todos, id, { tier: "global" });
}
// #endregion writing-durability-vue

const title = ref("");

async function handleSubmit(event: SubmitEvent) {
  event.preventDefault();
  if (!title.value.trim()) {
    return;
  }

  await addTodo(title.value.trim());
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
