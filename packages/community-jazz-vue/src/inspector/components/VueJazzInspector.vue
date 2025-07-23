<template>
  <div class="vue-jazz-inspector">
    <!-- Floating Inspector Button -->
    <InspectorButton
      v-if="!isOpen && isReady"
      :position="buttonPositionForJSX"
      @click="openInspector"
    />
    
    <!-- Inspector Panel -->
    <InspectorPanel
      :is-open="isOpen"
      :local-node="localNode"
      :account-id="accountId"
      :is-authenticated="isAuthenticated"
      :current-co-value-id="currentCoValueId"
      :navigation-path="navigationPath"
      :can-go-back="canGoBack"
      @close="closeInspector"
      @inspect="handleInspectCoValue"
      @inspect-account="handleInspectAccount"
      @navigate-to-covalue="handleNavigateToCoValue"
      @set-page="handleSetPage"
      @go-back="goBack"
      @navigate-to-index="navigateToIndex"
    />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useJazzInspector } from "../composables/useJazzInspector.js";
import { InspectorButton } from "../viewer/inspector-button.js";
import InspectorPanel from "./InspectorPanel.vue";

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

const props = withDefaults(defineProps<Props>(), {
  position: "bottom-right",
});

// Use the composable for all Jazz Inspector logic
const {
  isOpen,
  currentCoValueId,
  navigationPath,
  canGoBack,
  localNode,
  accountId,
  isAuthenticated,
  isReady,
  openInspector,
  closeInspector,
  inspectCoValue,
  inspectMyAccount,
  navigateToCoValue,
  setPage,
  navigateToIndex,
  goBack,
} = useJazzInspector();

// Determine button position based on prop (panel is always bottom-sheet)
const buttonPositionForJSX = computed(() => {
  const pos = props.position;
  // Convert hyphenated position to space-separated for JSX component
  if (pos === "bottom-right") return "bottom right";
  if (pos === "bottom-left") return "bottom left";
  if (pos === "top-right") return "top right";
  if (pos === "top-left") return "top left";
  if (pos === "right") return "right";
  if (pos === "left") return "left";
  return "bottom right"; // default
});

// Event handlers
const handleInspectCoValue = (coValueId: string) => {
  navigateToCoValue(coValueId, "CoValue");
};

const handleInspectAccount = () => {
  inspectMyAccount();
};

const handleNavigateToCoValue = (id: string, title: string) => {
  navigateToCoValue(id, title);
};

const handleSetPage = (id: string, title: string) => {
  setPage(id, title);
};
</script>

<style scoped>
.vue-jazz-inspector {
  /* Container for the inspector components */
}
</style>
