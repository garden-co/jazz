<!-- #region reading-one-vue -->
<script setup lang="ts">
import { useOne } from "jazz-tools/vue";
import { app } from "../schema.js";

const props = defineProps<{ id: string }>();

const { data: todo } = useOne(() => app.todos.where({ id: props.id }));
// undefined = loading; null = no row matches or you can't read it
</script>

<template>
  <p v-if="todo === undefined">Loading…</p>
  <p v-else-if="todo === null">Todo not found.</p>
  <h1 v-else>{{ todo.title }}</h1>
</template>
<!-- #endregion reading-one-vue -->
