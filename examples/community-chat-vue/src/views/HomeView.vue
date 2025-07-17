<template>
  <div v-if="!me">Loading...</div>
  <div v-else>Creating a new chat...</div>
</template>

<script setup lang="ts">
import { useAccount, useDemoAuth } from "community-jazz-vue";
import { Group } from "jazz-tools";
import { watch } from "vue";
import { useRouter } from "vue-router";
import { Chat } from "../schema";

const router = useRouter();
const { me } = useAccount();
const demoAuth = useDemoAuth();

watch(demoAuth, ({ state }) => {
  if (state === "anonymous") {
    return;
  }

  if (me.value) {
    const group = Group.create({ owner: me.value });
    group.addMember("everyone", "writer");
    const chat = Chat.create([], { owner: group });
    router.push(`/chat/${chat.id}`);
  }
});
</script>
