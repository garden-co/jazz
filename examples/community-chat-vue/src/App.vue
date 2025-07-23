<template>
  <AppContainer>
    <TopBar v-if="me">
      <p>{{ me.profile?.name }}</p>
      <button @click="logoutHandler">Log out</button>
    </TopBar>
    <router-view />
  </AppContainer>
</template>

<script setup lang="ts">
import { useAccount } from "community-jazz-vue";
import { useRouter } from "vue-router";
import AppContainer from "./components/AppContainer.vue";
import TopBar from "./components/TopBar.vue";

const { me, logOut } = useAccount();
const router = useRouter();

async function logoutHandler() {
  await logOut();
  router.push("/");
}
</script>
