import type { CoID, LocalNode, RawAccount, RawCoValue } from "cojson";
import { Account } from "jazz-tools";
import { computed, markRaw, ref, shallowRef } from "vue";
import { useIsAuthenticated, useJazzContext } from "../../index.js";

// Navigation types
export interface PageInfo {
  id: CoID<RawCoValue>;
  title: string;
  type?: string;
}

export function useJazzInspector() {
  // Use shallowRef to avoid deep reactivity on Jazz objects
  const isOpen = ref(false);
  const currentCoValueId = ref<string>("");

  // Navigation state
  const navigationPath = ref<PageInfo[]>([]);
  const currentPageIndex = ref(-1);

  // Get Jazz context and authentication state
  const context = useJazzContext<Account>();
  const isAuthenticated = useIsAuthenticated();

  // Safely extract Jazz data without proxy conflicts
  const jazzData = computed(() => {
    try {
      const ctx = context.value;
      if (!ctx) return null;

      // Mark Jazz objects as raw to prevent Vue reactivity
      const localNode = markRaw(ctx.node);
      const me = "me" in ctx ? markRaw(ctx.me) : undefined;

      // Access _raw property directly (like React version)
      const accountId = me?._raw?.id;

      return {
        localNode,
        me,
        accountId,
        hasContext: !!ctx,
      };
    } catch (error) {
      console.warn("[useJazzInspector] Error accessing Jazz data:", error);
      return null;
    }
  });

  // Safe getters
  const localNode = computed(() => jazzData.value?.localNode);
  const me = computed(() => jazzData.value?.me);
  const accountId = computed(() => jazzData.value?.accountId);
  const hasContext = computed(() => jazzData.value?.hasContext ?? false);
  const isReady = computed(() => !!(localNode.value && accountId.value));

  // Actions
  const openInspector = () => {
    isOpen.value = true;
  };

  const closeInspector = () => {
    isOpen.value = false;
  };

  const toggleInspector = () => {
    isOpen.value = !isOpen.value;
  };

  const inspectCoValue = (coValueId: string) => {
    currentCoValueId.value = coValueId;
    if (!isOpen.value) {
      openInspector();
    }
  };

  const inspectMyAccount = () => {
    if (accountId.value && isAuthenticated.value) {
      navigateToCoValue(accountId.value, "My Account");
    }
  };

  // Navigation functions
  const navigateToCoValue = (coValueId: string, title?: string) => {
    const pageInfo: PageInfo = {
      id: coValueId as CoID<RawCoValue>,
      title: title || coValueId,
      type: "covalue",
    };

    // Add to navigation path
    navigationPath.value.push(pageInfo);
    currentPageIndex.value = navigationPath.value.length - 1;
    currentCoValueId.value = coValueId;

    if (!isOpen.value) {
      openInspector();
    }
  };

  // Set page (replace current navigation like React version)
  const setPage = (coValueId: string, title?: string) => {
    const pageInfo: PageInfo = {
      id: coValueId as CoID<RawCoValue>,
      title: title || "Root",
      type: "covalue",
    };

    // Replace entire navigation path with single page
    navigationPath.value = [pageInfo];
    currentPageIndex.value = 0;
    currentCoValueId.value = coValueId;

    if (!isOpen.value) {
      openInspector();
    }
  };

  const navigateToIndex = (index: number) => {
    if (index === -1) {
      // Home button clicked - clear all navigation
      clearNavigation();
    } else if (index >= 0 && index < navigationPath.value.length) {
      currentPageIndex.value = index;
      // Remove pages after this index
      navigationPath.value = navigationPath.value.slice(0, index + 1);
      const currentPage = navigationPath.value[index];
      if (currentPage) {
        currentCoValueId.value = currentPage.id;
      }
    }
  };

  const goBack = () => {
    if (currentPageIndex.value > 0) {
      navigateToIndex(currentPageIndex.value - 1);
    }
  };

  const clearNavigation = () => {
    navigationPath.value = [];
    currentPageIndex.value = -1;
    currentCoValueId.value = "";
  };

  // Current page info
  const currentPage = computed(() => {
    if (
      currentPageIndex.value >= 0 &&
      currentPageIndex.value < navigationPath.value.length
    ) {
      return navigationPath.value[currentPageIndex.value];
    }
    return null;
  });

  const canGoBack = computed(() => currentPageIndex.value > 0);

  return {
    // State
    isOpen,
    currentCoValueId,

    // Navigation
    navigationPath,
    currentPageIndex,
    currentPage,
    canGoBack,

    // Jazz data
    localNode,
    me,
    accountId,
    isAuthenticated,
    isReady,

    // Actions
    openInspector,
    closeInspector,
    toggleInspector,
    inspectCoValue,
    inspectMyAccount,
    navigateToCoValue,
    setPage,
    navigateToIndex,
    goBack,
    clearNavigation,
  };
}
