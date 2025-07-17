<template>
  <JazzVueProvider
    :sync="{
      peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    }"
  >
    <DemoAuthBasicUI appName="Jazz Vue Chat">
      <App />
    </DemoAuthBasicUI>
    <JazzInspector />
  </JazzVueProvider>
</template>

<script setup lang="ts">
import {
  DemoAuthBasicUI,
  JazzInspector,
  JazzVueProvider,
} from "community-jazz-vue";
import App from "./App.vue";
import { apiKey } from "./apiKey";

import "community-jazz-vue/dist/community-jazz-vue.css";
</script>
