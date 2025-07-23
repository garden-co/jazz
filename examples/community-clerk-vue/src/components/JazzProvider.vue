<script setup lang="ts">
import { useClerk } from "@clerk/vue";
import { JazzInspector, JazzVueProviderWithClerk } from "community-jazz-vue";
import { apiKey } from "../apiKey";

const clerk = useClerk();
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
    <JazzInspector position="bottom-left"/>
  </JazzVueProviderWithClerk>
</template>
