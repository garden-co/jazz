<template>
  <div class="accept-invite-container">
    <div class="invite-card">
      <h2>Accepting Invite...</h2>
      <p v-if="isProcessing">Processing your invitation...</p>
      <p v-else-if="error" class="error">{{ error }}</p>
      <p v-else-if="success" class="success">
        Invitation accepted! You now have access to the shared project.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useAcceptInvite } from "community-jazz-vue";
import { onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { TodoProject } from "../schema";

const router = useRouter();
const isProcessing = ref(true);
const error = ref<string | null>(null);
const success = ref(false);

// Use the Jazz Vue invite acceptance hook
useAcceptInvite({
  invitedObjectSchema: TodoProject,
  forValueHint: "project",
  onAccept: (projectId: string) => {
    console.log("Invite accepted for project:", projectId);
    success.value = true;
    isProcessing.value = false;

    // Redirect to the project after a short delay
    setTimeout(() => {
      router.push(`/project/${projectId}`);
    }, 2000);
  },
});

onMounted(() => {
  // Set a timeout to show error if invite processing takes too long
  setTimeout(() => {
    if (isProcessing.value) {
      error.value =
        "Failed to process invitation. Please check the invite link.";
      isProcessing.value = false;
    }
  }, 10000); // 10 second timeout
});
</script>

<style scoped>
.accept-invite-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 60vh;
  padding: 2rem;
}

.invite-card {
  background-color: #fff;
  border-radius: 8px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  padding: 2rem;
  text-align: center;
  max-width: 400px;
  width: 100%;
}

h2 {
  color: #2c3e50;
  margin-bottom: 1rem;
}

p {
  margin: 1rem 0;
  font-size: 1rem;
}

.error {
  color: #dc3545;
  font-weight: 500;
}

.success {
  color: #28a745;
  font-weight: 500;
}
</style>
