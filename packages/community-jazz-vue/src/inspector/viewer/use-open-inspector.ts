import { ref, watch } from "vue";

const STORAGE_KEY = "jazz-inspector-open";

export function useOpenInspector() {
  // Always start closed to prevent race conditions with context initialization
  const open = ref<boolean>(false);
  const hasRestoredFromStorage = ref<boolean>(false);

  console.log("[useOpenInspector] Hook initialized with open:", open.value);

  // Restore from localStorage after component is stable
  const restoreFromStorage = () => {
    if (hasRestoredFromStorage.value || typeof window === "undefined") {
      return;
    }

    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const storedValue = JSON.parse(stored);
        console.log(
          "[useOpenInspector] Restoring from localStorage:",
          storedValue,
        );
        // Only restore if it was open, and only if we haven't already restored
        if (storedValue === true && !hasRestoredFromStorage.value) {
          open.value = true;
        }
      }
      hasRestoredFromStorage.value = true;
    } catch (error) {
      console.warn(
        "[useOpenInspector] Error reading from localStorage:",
        error,
      );
      hasRestoredFromStorage.value = true;
    }
  };

  // Update localStorage when open state changes (only in browser)
  watch(
    open,
    (newValue, oldValue) => {
      console.log("[useOpenInspector] Open state changed:", {
        old: oldValue,
        new: newValue,
        timestamp: new Date().toISOString(),
      });

      if (typeof window !== "undefined") {
        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(newValue));
          console.log("[useOpenInspector] Saved to localStorage:", newValue);
        } catch (error) {
          console.warn(
            "[useOpenInspector] Error saving to localStorage:",
            error,
          );
        }
      }
    },
    { immediate: true },
  );

  const setOpen = (value: boolean) => {
    console.log("[useOpenInspector] setOpen called with:", value);

    try {
      // If opening for the first time, try to restore from storage first
      if (value === true && !hasRestoredFromStorage.value) {
        restoreFromStorage();
      }

      open.value = value;
    } catch (error) {
      console.error("[useOpenInspector] Error in setOpen:", error);
      // Fallback to setting the value directly
      open.value = value;
    }
  };

  return [open, setOpen, restoreFromStorage] as const;
}
