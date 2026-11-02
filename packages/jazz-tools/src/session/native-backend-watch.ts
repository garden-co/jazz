import { AppState } from "react-native";
import type { BackendWatchEvents } from "./backend-watch.js";

/** Ask the backend again whenever the app comes back to the foreground. */
export const nativeBackendWatchEvents: BackendWatchEvents = {
  onFocus(check) {
    const subscription = AppState.addEventListener?.("change", (state) => {
      if (state === "active") check();
    });
    return () => subscription?.remove();
  },
  isVisible: () => AppState.currentState === "active",
};
