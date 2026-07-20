<template>
  <section class="todo-widget">
    <h2>Your todos</h2>
    <form @submit.prevent="add">
      <input type="text" v-model="newTitle" placeholder="Add a task" aria-label="New todo" />
      <button type="submit">Add</button>
    </form>
    <ul>
      <li v-for="todo in todos" :key="todo.id" :class="{ done: todo.done }">
        <label>
          <input
            type="checkbox"
            :checked="todo.done"
            @change="db.update(app.todos, todo.id, { done: !todo.done })"
          />
          <span>{{ todo.title }}</span>
        </label>
        <button type="button" aria-label="Delete" @click="db.delete(app.todos, todo.id)">×</button>
      </li>
    </ul>
  </section>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { useAll, useDb } from "jazz-tools/vue";
import { app } from "~/schema";

const db = useDb();
const todos = useAll(app.todos);
const newTitle = ref("");

function add() {
  const title = newTitle.value.trim();
  if (!title) return;
  db.insert(app.todos, { title, done: false });
  newTitle.value = "";
}
</script>
