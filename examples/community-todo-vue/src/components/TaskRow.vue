<template>
  <div class="task-row">
    <div class="task-checkbox-container">
      <input
        type="checkbox"
        :checked="task?.done"
        @change="handleToggle"
        class="task-checkbox"
      />
    </div>
    <div class="task-content">
      <div class="task-text-container">
        <span 
          v-if="task?.text" 
          :class="['task-text', { 'task-done': task?.done }]"
        >
          {{ task.text }}
        </span>
        <div v-else class="skeleton skeleton-text"></div>
        
        <span 
          v-if="task?.$jazz.getEdits().text?.by?.profile?.name"
          class="task-author"
          :style="getAuthorStyle(task.$jazz.getEdits().text.by?.id ?? '')"
        >
          {{ task.$jazz.getEdits().text.by?.profile?.name }}
        </span>
        <div v-else class="skeleton skeleton-author"></div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Loaded } from "jazz-tools";
import type { Task } from "../schema";

interface Props {
  task: Loaded<typeof Task> | undefined;
}

const props = defineProps<Props>();

const handleToggle = (event: Event) => {
  const target = event.target as HTMLInputElement;
  if (props.task) {
    props.task.done = target.checked;
  }
};

// Generate unique colors for authors
const getAuthorStyle = (authorId: string) => {
  // Simple hash function to generate consistent colors
  let hash = 0;
  for (let i = 0; i < authorId.length; i++) {
    const char = authorId.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  const hue = Math.abs(hash) % 360;
  const isDark = window.matchMedia("(prefers-color-scheme: dark)").matches;

  return {
    color: `hsl(${hue}, 70%, ${isDark ? "80%" : "20%"})`,
    backgroundColor: `hsl(${hue}, 70%, ${isDark ? "20%" : "80%"})`,
  };
};
</script>

<style scoped>
.task-row {
  display: flex;
  align-items: center;
  border-bottom: 1px solid #e5e7eb;
  transition: background-color 0.2s;
  min-height: 60px;
}

.task-row:hover {
  background: #f9fafb;
}

.task-done {
  display: flex;
  align-items: center;
  justify-content: left;
  width: 60px;
  flex-shrink: 0;
}

.task-checkbox-container {
  display: flex;
  align-items: center;
  justify-content: left;
  padding-left: 1rem;
  width: 60px;
  flex-shrink: 0;
}

.task-checkbox {
  width: 1.25rem;
  height: 1.25rem;
  cursor: pointer;
}

.task-content {
  padding: 1rem;
  display: flex;
  align-items: center;
  flex: 1;
}

.task-text-container {
  display: flex;
  align-items: center;
  width: 100%;
  gap: 1rem;
}

.task-text {
  font-size: 1rem;
  color: #374151;
  flex: 1;
  text-align: left;
}

.task-text.task-done {
  text-decoration: line-through;
  color: #9ca3af;
}

.task-author {
  padding: 0.25rem 0.5rem;
  border-radius: 9999px;
  font-size: 0.75rem;
  font-weight: 500;
  white-space: nowrap;
  flex-shrink: 0;
  margin-left: auto;
}

.skeleton {
  background: #e5e7eb;
  border-radius: 0.25rem;
  animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}

.skeleton-text {
  height: 1rem;
  width: 200px;
}

.skeleton-author {
  height: 1rem;
  width: 50px;
  border-radius: 9999px;
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
}
</style>
