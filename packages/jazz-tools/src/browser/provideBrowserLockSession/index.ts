import { MockSessionProvider } from "jazz-tools";
import { BrowserSessionProvider } from "./BrowserSessionProvider";

export function getBrowserLockSessionProvider() {
  if (typeof navigator === "undefined" || !navigator.locks?.request) {
    // Fallback to random session ID for each tab session
    return new MockSessionProvider();
  }

  return new BrowserSessionProvider();
}
