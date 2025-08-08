<template>
  <button
    @click="handleInvite"
    class="invite-button"
    :disabled="!value"
    title="Share this project"
  >
    <svg
      class="invite-icon"
      fill="none"
      stroke="currentColor"
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
    >
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.367 2.684 3 3 0 00-5.367-2.684z"
      ></path>
    </svg>
    Share
  </button>
</template>

<script setup lang="ts">
import { createInviteLink } from "community-jazz-vue";
import type { CoValue } from "jazz-tools";

interface Props {
  value: CoValue | null | undefined;
  valueHint: string;
}

const props = defineProps<Props>();

const handleInvite = () => {
  if (!props.value) return;

  try {
    const inviteLink = createInviteLink(props.value, "writer", {
      valueHint: props.valueHint,
    });

    navigator.clipboard
      .writeText(inviteLink)
      .then(() => {
        alert(
          `Invite link copied to clipboard!\n\nShare this link to give others access to this ${props.valueHint}.`,
        );
      })
      .catch(() => {
        // Fallback if clipboard API fails
        prompt("Copy this invite link:", inviteLink);
      });
  } catch (error) {
    console.error("Failed to create invite link:", error);
    alert("Failed to create invite link. Please try again.");
  }
};
</script>

<style scoped>
.invite-button {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  background: #3b82f6;
  color: white;
  border: none;
  border-radius: 0.375rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.invite-button:hover:not(:disabled) {
  background: #2563eb;
}

.invite-button:disabled {
  background: #9ca3af;
  cursor: not-allowed;
}

.invite-icon {
  width: 1rem;
  height: 1rem;
}
</style>
