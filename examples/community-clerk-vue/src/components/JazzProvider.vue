<script setup lang="ts">
import { useClerk } from "@clerk/vue";
import { JazzVueProviderWithClerk } from "community-jazz-vue";
import { h } from "vue";

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
    <component
      :is="h('jazz-inspector', {
        style: { position: 'fixed', bottom: '20px', left: '20px', zIndex: 9999 }
      })"
    />
  </JazzVueProviderWithClerk>
  <div v-else>
    <p>Loading Clerk...</p>
  </div>
</template>
