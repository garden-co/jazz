<template>
  <VueJazzInspector :position="position" />
</template>

<script setup lang="ts">
import VueJazzInspector from "./components/VueJazzInspector.vue";

interface Props {
  position?:
    | "bottom-right"
    | "bottom-left"
    | "top-right"
    | "top-left"
    | "right"
    | "left"
    | "bottom";
}

withDefaults(defineProps<Props>(), {
  position: "bottom-right",
});
</script>

<style scoped>
/* Main Jazz Inspector component using canonical Vue implementation */
</style>
