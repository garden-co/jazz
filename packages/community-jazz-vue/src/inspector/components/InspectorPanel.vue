<template>
  <div v-if="isOpen" class="jazz-inspector-panel">
    <div class="jazz-inspector-header">
      <!-- Breadcrumbs (like React version) -->
      <Breadcrumbs
        v-if="navigationPath.length > 0"
        :path="jsxNavigationPath"
        :on-breadcrumb-click="handleBreadcrumbClick"
      />

      <!-- CoValue ID Input (when navigating) -->
      <form v-if="navigationPath.length > 0" @submit.prevent="handleInspectCoValue" class="header-form">
        <input
          v-model="coValueInput"
          type="text"
          placeholder="co_z1234567890abcdef123456789"
          class="header-covalue-input"
        />
      </form>

      <button @click="closeInspector" class="close-button">×</button>
    </div>
    
    <!-- CoValue Viewer with proper group support -->
    <div v-if="navigationPath && navigationPath.length > 0 && currentCoValueId" class="covalue-section">
      <CoValueViewer
        :co-value-id="currentCoValueId"
        :local-node="localNode"
        @navigate-to-covalue="handleNavigateToCoValue"
      />
    </div>

    <!-- Initial form when no navigation (like React version) -->
    <div v-else class="jazz-inspector-content">
      <div class="initial-form">
        <h2>Jazz CoValue Inspector</h2>
        <form @submit.prevent="handleInspectCoValue" class="initial-form-content">
          <input
            v-model="coValueInput"
            type="text"
            placeholder="co_z1234567890abcdef123456789"
            class="covalue-input"
          />
          <button type="submit" class="inspect-button">
            Inspect CoValue
          </button>

          <p v-if="accountId && isAuthenticated" class="or-text">or</p>

          <button
            v-if="accountId && isAuthenticated"
            @click="inspectMyAccount"
            type="button"
            class="action-button"
          >
            Inspect My Account
          </button>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { CoID, LocalNode, RawAccount, RawCoValue } from "cojson";
import { computed, ref } from "vue";
import { Breadcrumbs } from "../viewer/breadcrumbs.js";
import CoValueViewer from "./CoValueViewer.vue";

interface Props {
  isOpen?: boolean;
  localNode?: LocalNode;
  accountId?: CoID<RawAccount>;
  isAuthenticated?: boolean;
  currentCoValueId?: string;
  navigationPath?: Array<{ id: string; title: string; type?: string }>;
  canGoBack?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  isOpen: false,
  isAuthenticated: false,
  navigationPath: () => [],
  canGoBack: false,
});

const emit = defineEmits<{
  close: [];
  inspect: [coValueId: string];
  "inspect-account": [];
  "navigate-to-covalue": [id: string, title: string];
  "set-page": [id: string, title: string];
  "go-back": [];
  "navigate-to-index": [index: number];
}>();

const coValueInput = ref("");

const closeInspector = () => {
  emit("close");
};

const handleInspectCoValue = () => {
  if (coValueInput.value.trim()) {
    // If we're in navigation mode (header input), replace current page
    if (props.navigationPath && props.navigationPath.length > 0) {
      emit("set-page", coValueInput.value.trim(), "CoValue");
      coValueInput.value = "";
    } else {
      // If we're in initial mode, start navigation
      emit("inspect", coValueInput.value.trim());
    }
  }
};

const inspectMyAccount = () => {
  if (props.accountId) {
    coValueInput.value = props.accountId;
    emit("inspect-account");
  }
};

const handleNavigateToCoValue = (id: string, title: string) => {
  emit("navigate-to-covalue", id, title);
};

// Convert Vue navigation path to JSX PageStack format
const jsxNavigationPath = computed(() => {
  return (
    props.navigationPath?.map((page) => ({
      coId: page.id as CoID<RawCoValue>,
      name: page.title,
    })) || []
  );
});

const handleGoBack = () => {
  emit("go-back");
};

// Remove unused JSX handlers

const handleBreadcrumbClick = (index: number) => {
  if (index === -1) {
    // Home button clicked - clear navigation
    emit("navigate-to-index", -1);
  } else {
    emit("navigate-to-index", index);
  }
};
</script>

<style scoped>
.jazz-inspector-panel {
  position: fixed;
  height: 50vh;
  max-height: 800px;
  display: flex;
  flex-direction: column;
  bottom: 0;
  left: 0;
  width: 100%;
  background-color: var(--j-background);
  border-top: 1px solid var(--j-border-color);
  color: var(--j-text-color);
  z-index: 999;
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
}

.jazz-inspector-header {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0 0.75rem;
  margin: 0.75rem 0;
  flex-shrink: 0;
}

.header-form {
  width: 24rem;
}

.header-covalue-input {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  background-color: var(--j-background);
  color: var(--j-text-color);
}

.jazz-inspector-header h3 {
  margin: 0;
  color: var(--j-text-color-strong);
  font-size: 1.125rem;
  font-weight: 500;
}

.close-button {
  background: none;
  border: none;
  font-size: 1.25rem;
  cursor: pointer;
  color: var(--j-text-color);
  padding: 0.25rem;
  width: 1.5rem;
  height: 1.5rem;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: var(--j-radius-sm);
  margin-left: auto;
}

.close-button:hover {
  background: var(--j-foreground);
  color: var(--j-text-color-strong);
}

.jazz-inspector-content {
  padding: 0 0.75rem 0.75rem;
  overflow-y: auto;
  flex: 1;
  min-height: 0;
}

.initial-form {
  display: flex;
  flex-direction: column;
  justify-content: center;
  height: 100%;
  max-width: 24rem;
  margin: 0 auto;
  position: relative;
  top: -1.5rem;
}

.initial-form h2 {
  text-align: center;
  color: var(--j-text-color-strong);
  margin-bottom: 2rem;
  font-size: 1.5rem;
  font-weight: 600;
}

.initial-form-content {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.or-text {
  text-align: center;
  color: var(--j-text-color);
  margin: 0.5rem 0;
}

.covalue-input {
  padding: 0.75rem;
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 0.875rem;
  background-color: var(--j-background);
  color: var(--j-text-color);
  min-width: 21rem;
}

.inspect-button,
.action-button {
  padding: 0.5rem 1rem;
  background: var(--j-primary-color);
  color: white;
  border: none;
  border-radius: var(--j-radius-md);
  cursor: pointer;
  font-weight: 500;
  font-size: 0.875rem;
}

.inspect-button:hover,
.action-button:hover {
  background: color-mix(in srgb, var(--j-primary-color) 80%, black);
}

/* CoValue section styling */
.covalue-section {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  background: var(--j-background);
  color: var(--j-text-color);
  padding: 0.75rem;
}
</style>
