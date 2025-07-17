<template>
  <div class="project-container">
    <div class="project-header">
      <h1>
        <template v-if="project?.title">
          {{ project.title }} 
          <span class="project-id">({{ project.id }})</span>
        </template>
        <div v-else class="skeleton skeleton-title"></div>
      </h1>
      <InviteButton :value="project" valueHint="project" />
    </div>

    <div class="tasks-table">
      <div class="table-header">
        <div class="header-cell header-done">Done</div>
        <div class="header-cell header-task">Task</div>
      </div>
      
      <div class="table-body">
        <TaskRow
          v-for="task in project?.tasks"
          :key="task?.id"
          :task="task"
        />
        <NewTaskRow :createTask="createTask" :disabled="!project" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useCoState } from "community-jazz-vue";
import { CoPlainText } from "jazz-tools";
import InviteButton from "../components/InviteButton.vue";
import NewTaskRow from "../components/NewTaskRow.vue";
import TaskRow from "../components/TaskRow.vue";
import { Task, TodoProject } from "../schema";

interface Props {
  projectId: string;
}

const props = defineProps<Props>();

// `useCoState()` reactively subscribes to updates to a CoValue's
// content - whether we create edits locally, load persisted data, or receive
// sync updates from other devices or participants!
// It also recursively resolves and subscribes to all referenced CoValues.
const project = useCoState(TodoProject, props.projectId, {
  resolve: {
    tasks: {
      $each: {
        text: true,
      },
    },
  },
});

// `createTask` is similar to `createProject` we saw earlier, creating a new CoMap
// for a new task (in the same group as the project), and then
// adding that as an item to the project's list of tasks.
const createTask = (text: string) => {
  if (!project.value?.tasks || !text) return;

  const task = Task.create(
    {
      done: false,
      text: CoPlainText.create(text, project.value._owner),
      version: 1,
    },
    project.value._owner,
  );

  // push will cause useCoState to rerender this component, both here and on other devices
  project.value.tasks.push(task);
};
</script>

<style scoped>
.project-container {
  max-width: 1000px;
  margin: 0 auto;
  padding: 2rem;
}

.project-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 2rem;
  gap: 1rem;
}

h1 {
  font-size: 1.875rem;
  font-weight: 600;
  color: #1f2937;
  margin: 0;
}

.project-id {
  font-size: 0.875rem;
  font-weight: 400;
  color: #6b7280;
}

.skeleton {
  background: #e5e7eb;
  border-radius: 0.25rem;
  animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
}

.skeleton-title {
  height: 2rem;
  width: 200px;
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.5;
  }
}

.tasks-table {
  background: white;
  border-radius: 0.5rem;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
  overflow: hidden;
}

.table-header {
  display: grid;
  grid-template-columns: 60px 1fr;
  background: #f9fafb;
  border-bottom: 1px solid #e5e7eb;
}

.header-cell {
  padding: 1rem;
  font-weight: 600;
  color: #374151;
  font-size: 0.875rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.table-body {
  display: flex;
  flex-direction: column;
}
</style>
