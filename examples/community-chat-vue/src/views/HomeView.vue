<template>
  <div v-if="!me">Loading...</div>
  <div v-else>Creating a new chat...</div>
</template>

<script setup lang="ts">
import { useAccount, useIsAuthenticated } from "community-jazz-vue";
import { Group } from "jazz-tools";
import { watch } from "vue";
import { useRouter } from "vue-router";
import { Chat } from "../schema";

const router = useRouter();
const { me } = useAccount();
const isAuthenticated = useIsAuthenticated();

watch(
  [me, isAuthenticated],
  ([currentMe, authenticated]) => {
    if (currentMe && authenticated) {
      try {
        const group = Group.create({ owner: currentMe });
        group.addMember("everyone", "writer");
        const chat = Chat.create([], { owner: group });
        router.push(`/chat/${chat.$jazz.id}`);
      } catch (error) {
        console.error("Failed to create chat:", error);
      }
    }
  },
  { immediate: true },
);
</script>
