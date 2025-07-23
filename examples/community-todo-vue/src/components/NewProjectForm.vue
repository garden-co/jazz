<template>
  <div class="new-project-form">
    <form @submit.prevent="handleSubmit">
      <div class="form-group">
        <label for="project-title" class="form-label">Create New Project</label>
        <input
          id="project-title"
          v-model="title"
          type="text"
          placeholder="New project title"
          class="form-input"
          required
        />
      </div>
      <button type="submit" class="submit-button" :disabled="!title.trim()">
        Create Project
      </button>
    </form>
  </div>
</template>

<script setup lang="ts">
import { useAccount } from "community-jazz-vue";
import { Group, co } from "jazz-tools";
import { ref } from "vue";
import { useRouter } from "vue-router";
import { Task, TodoAccount, TodoProject } from "../schema";

const { me } = useAccount(TodoAccount, {
  resolve: { root: { projects: { $each: { $onError: null } } } },
});

const router = useRouter();
const title = ref("");

const handleSubmit = () => {
  if (!me.value || !me.value.root?.projects || !title.value.trim()) return;

  // To create a new todo project, we first create a `Group`,
  // which is a scope for defining access rights (reader/writer/admin)
  // of its members, which will apply to all CoValues owned by that group.
  const projectGroup = Group.create({ owner: me.value });

  // Then we create an empty todo project within that group
  const project = TodoProject.create(
    {
      title: title.value,
      tasks: co.list(Task).create([], { owner: projectGroup }),
    },
    { owner: projectGroup },
  );

  me.value.root.projects.push(project);

  router.push(`/project/${project.id}`);
  title.value = "";
};
</script>

<style scoped>
.new-project-form {
  max-width: 500px;
  margin: 0 auto;
  padding: 2rem;
  background: white;
  border-radius: 0.5rem;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
}

.form-group {
  margin-bottom: 1.5rem;
}

.form-label {
  display: block;
  font-size: 1.125rem;
  font-weight: 500;
  color: #374151;
  margin-bottom: 0.5rem;
}

.form-input {
  width: 100%;
  padding: 0.75rem 1rem;
  border: 1px solid #d1d5db;
  border-radius: 0.375rem;
  font-size: 1rem;
  transition: border-color 0.2s;
}

.form-input:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.submit-button {
  width: 100%;
  padding: 0.75rem 1.5rem;
  background: #3b82f6;
  color: white;
  border: none;
  border-radius: 0.375rem;
  font-size: 1rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.submit-button:hover:not(:disabled) {
  background: #2563eb;
}

.submit-button:disabled {
  background: #9ca3af;
  cursor: not-allowed;
}
</style>
