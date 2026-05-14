<script setup lang="ts">
import { computed, ref } from "vue";
import { useAll, useDb, useSession } from "jazz-tools/vue";
import { toast } from "vue-sonner";
import { app } from "./lib/schema.js";

const db = useDb();
// #region reading-reactive-vue
const todos = useAll(app.todos);
// #endregion reading-reactive-vue
const session = useSession();
const sessionUserId = computed(() => session?.user_id ?? null);
const title = ref("");

function handleSubmit(e: Event) {
  e.preventDefault();
  if (!title.value.trim() || !sessionUserId.value) return;
  db.insert(app.todos, {
    title: title.value.trim(),
    done: false,
    owner_id: sessionUserId.value,
  });
  title.value = "";
}

function toggleTodo(todo: { id: string; done: boolean }, event: Event) {
  const checkbox = event.currentTarget as HTMLInputElement;

  try {
    db.update(app.todos, todo.id, { done: !todo.done });
  } catch {
    checkbox.checked = todo.done;
    toast.error("You don't have permission to update this task");
  }
}

function deleteTodo(todoId: string) {
  try {
    db.delete(app.todos, todoId);
  } catch {
    toast.error("You don't have permission to delete this task");
  }
}
</script>

<template>
  <form @submit="handleSubmit">
    <input type="text" v-model="title" placeholder="What needs to be done?" required />
    <button type="submit" :disabled="!sessionUserId">Add</button>
  </form>
  <ul id="todo-list">
    <li v-for="todo in todos ?? []" :key="todo.id" :class="{ done: todo.done }">
      <input
        type="checkbox"
        :checked="todo.done"
        @change="(event) => toggleTodo(todo, event)"
        class="toggle"
      />
      <span>{{ todo.title }}</span>
      <small v-if="todo.description">{{ todo.description }}</small>
      <button class="delete-btn" @click="deleteTodo(todo.id)">&times;</button>
    </li>
  </ul>
</template>
