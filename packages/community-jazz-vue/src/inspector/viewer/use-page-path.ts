import { CoID, RawCoValue } from "cojson";
import { computed, ref, watch } from "vue";
import { PageInfo } from "./types.js";

const STORAGE_KEY = "jazz-inspector-paths";

export function usePagePath(defaultPath?: PageInfo[]) {
  const getInitialPath = () => {
    if (typeof window === "undefined") return defaultPath || [];

    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        return JSON.parse(stored);
      }
    } catch (e) {
      console.warn("Failed to parse stored path:", e);
    }
    return defaultPath || [];
  };

  const path = ref<PageInfo[]>(getInitialPath());

  const updatePath = (newPath: PageInfo[]) => {
    path.value = newPath;
    if (typeof window !== "undefined") {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(newPath));
      } catch (e) {
        console.warn("Failed to save path to localStorage:", e);
      }
    }
  };

  watch(
    () => defaultPath,
    (newDefaultPath) => {
      if (
        newDefaultPath &&
        JSON.stringify(path.value) !== JSON.stringify(newDefaultPath)
      ) {
        updatePath(newDefaultPath);
      }
    },
  );

  const addPages = (newPages: PageInfo[]) => {
    updatePath([...path.value, ...newPages]);
  };

  const goToIndex = (index: number) => {
    updatePath(path.value.slice(0, index + 1));
  };

  const setPage = (coId: CoID<RawCoValue>) => {
    updatePath([{ coId, name: "Root" }]);
  };

  const goBack = () => {
    if (path.value.length > 1) {
      updatePath(path.value.slice(0, path.value.length - 1));
    }
  };

  return {
    path: computed(() => path.value),
    setPage,
    addPages,
    goToIndex,
    goBack,
  };
}
