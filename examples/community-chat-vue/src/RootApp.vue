<script setup lang="ts">
import { JazzVueProvider, PasskeyAuthBasicUI } from "community-jazz-vue";
import { h } from "vue";
import "jazz-tools/inspector/register-custom-element";
import App from "./App.vue";
import { apiKey } from "./apiKey";
</script>

<template>
  <JazzVueProvider
    :sync="{
      peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    }"
  >
    <PasskeyAuthBasicUI appName="Jazz Vue Chat">
      <App />
    </PasskeyAuthBasicUI>
    <component
      :is="h('jazz-inspector', {
        style: { position: 'fixed', left: '20px', bottom: '20px', zIndex: 9999 }
      })"
    />
  </JazzVueProvider>
</template>
