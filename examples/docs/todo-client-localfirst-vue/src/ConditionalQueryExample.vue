<!-- #region reading-conditional-query-vue -->
<script setup lang="ts">
  import { ref, computed } from "vue";
  import { useAll } from "jazz-tools/vue";
  import { app } from "../schema/app.js";

  const filter = ref<string | null>(null);
  const query = computed(() =>
    filter.value ? app.todos.where({ title: { contains: filter.value } }) : undefined,
  );
  const filtered = useAll(query);
</script>

<template>
  <input v-model="filter" placeholder="Filter by title" />
  <ul v-if="filtered">
    <li v-for="todo in filtered" :key="todo.id">{{ todo.title }}</li>
  </ul>
</template>
<!-- #endregion reading-conditional-query-vue -->
