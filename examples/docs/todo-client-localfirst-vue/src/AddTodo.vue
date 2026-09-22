<script setup lang="ts">
import { ref } from "vue";
import { useDb } from "jazz-tools/vue";
import { app } from "../schema.js";

const db = useDb();
const title = ref("");

function addTodo() {
  db.insert(app.todos, { title: title.value, done: false });
  title.value = "";
}
</script>

<template>
  <form @submit.prevent="addTodo">
    <input v-model="title" type="text" placeholder="What needs to be done?" />
    <button type="submit">Add</button>
  </form>
</template>
