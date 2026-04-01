<script setup lang="ts">
import { useAll } from "jazz-tools/vue";
import { app } from "../schema.js";
import TodoItem from "./TodoItem.vue";
import AddTodo from "./AddTodo.vue";

const todos = useAll(app.todos);
</script>

<template>
  <ul>
    <TodoItem v-for="todo in todos ?? []" :key="todo.id" :id="todo.id" />
  </ul>
  <AddTodo />
</template>
