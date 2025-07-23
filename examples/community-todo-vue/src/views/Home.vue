<template>
  <div class="home-container">
    <h1 v-if="me?.root?.projects?.length">My Projects</h1>
    
    <!-- Project List -->
    <div class="projects-list">
      <button
        v-for="project in me?.root?.projects"
        :key="project?.id"
        @click="navigateToProject(project?.id)"
        class="project-button"
      >
        {{ project?.title }}
      </button>
    </div>
    
    <!-- New Project Form -->
    <NewProjectForm />
  </div>
</template>

<script setup lang="ts">
import { useAccount } from "community-jazz-vue";
import { useRouter } from "vue-router";
import NewProjectForm from "../components/NewProjectForm.vue";
import { TodoAccount } from "../schema";

const { me } = useAccount(TodoAccount, {
  resolve: { root: { projects: { $each: { $onError: null } } } },
});

const router = useRouter();

const navigateToProject = (projectId: string | undefined) => {
  if (projectId) {
    router.push(`/project/${projectId}`);
  }
};
</script>

<style scoped>
.home-container {
  max-width: 800px;
  margin: 0 auto;
  padding: 2rem;
}

h1 {
  font-size: 2rem;
  font-weight: 600;
  color: #1f2937;
  margin-bottom: 2rem;
  text-align: center;
}

.projects-list {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  margin-bottom: 2rem;
}

.project-button {
  padding: 1rem 1.5rem;
  border: 1px solid #d1d5db;
  border-radius: 0.5rem;
  background: white;
  color: #374151;
  cursor: pointer;
  font-size: 1rem;
  text-align: left;
  transition: all 0.2s;
}

.project-button:hover {
  background: #f9fafb;
  border-color: #9ca3af;
  transform: translateY(-1px);
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
}

.project-button:active {
  transform: translateY(0);
}
</style>
