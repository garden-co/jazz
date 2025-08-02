<script setup lang="ts">
import { useClerk } from "@clerk/vue";
import { JazzVueProviderWithClerk } from "community-jazz-vue";

import { apiKey } from "../apiKey";

const clerk = useClerk();

import "jazz-tools/inspector/register-custom-element";
</script>

<template>
  <JazzVueProviderWithClerk
    v-if="clerk"
    :clerk="clerk"
    :sync="{
      peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    }"
  >
    <slot />
    <jazz-inspector
      style="position: fixed; bottom: 20px; left: 20px; z-index: 9999"
    />
  </JazzVueProviderWithClerk>
  <div v-else>
    <p>Loading Clerk...</p>
  </div>
</template>
