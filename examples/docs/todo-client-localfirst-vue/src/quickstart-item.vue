<script setup lang="ts">
import { computed } from "vue";
import { useDb, useAll } from "jazz-tools/vue";
import { app } from "../schema/app.js";

const props = defineProps<{ id: string }>();

const db = useDb();
const todos = useAll(() => app.todos.where({ id: props.id }).limit(1));
const todo = computed(() => todos.value?.[0]);
</script>

<template>
  <li v-if="todo" :class="{ done: todo.done }">
    <input
      type="checkbox"
      :checked="todo.done"
      @change="db.update(app.todos, props.id, { done: !todo.done })"
    />
    <span>{{ todo.title }}</span>
    <button @click="db.delete(app.todos, props.id)">&times;</button>
  </li>
</template>
