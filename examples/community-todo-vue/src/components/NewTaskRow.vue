<template>
  <div class="new-task-row">
    <div class="task-done">
      <input
        type="checkbox"
        disabled
        class="task-checkbox disabled"
      />
    </div>
    <div class="task-content">
      <form @submit.prevent="handleSubmit" class="task-form">
        <input
          v-model="taskText"
          type="text"
          placeholder="New task"
          class="task-input"
          :disabled="disabled"
        />
        <button
          type="submit"
          class="add-button"
          :disabled="disabled || !taskText.trim()"
        >
          Add
        </button>
      </form>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";

interface Props {
  createTask: (text: string) => void;
  disabled: boolean;
}

const props = defineProps<Props>();

const taskText = ref("");

const handleSubmit = () => {
  if (!taskText.value.trim() || props.disabled) return;

  props.createTask(taskText.value);
  taskText.value = "";
};
</script>

<style scoped>
.new-task-row {
  display: grid;
  grid-template-columns: 60px 1fr;
  border-bottom: 1px solid #e5e7eb;
  background: #f9fafb;
}

.task-done {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.task-checkbox {
  width: 1.25rem;
  height: 1.25rem;
}

.task-checkbox.disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.task-content {
  padding: 1rem;
  display: flex;
  align-items: center;
}

.task-form {
  display: flex;
  gap: 0.75rem;
  width: 100%;
  align-items: center;
}

.task-input {
  flex: 1;
  padding: 0.5rem 0.75rem;
  border: 1px solid #d1d5db;
  border-radius: 0.375rem;
  font-size: 1rem;
  transition: border-color 0.2s;
}

.task-input:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.task-input:disabled {
  background: #f3f4f6;
  color: #9ca3af;
  cursor: not-allowed;
}

.add-button {
  padding: 0.5rem 1rem;
  background: #3b82f6;
  color: white;
  border: none;
  border-radius: 0.375rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
  white-space: nowrap;
}

.add-button:hover:not(:disabled) {
  background: #2563eb;
}

.add-button:disabled {
  background: #9ca3af;
  cursor: not-allowed;
}
</style>
