"use client";

import { useEffect } from "react";
import { track } from "@vercel/analytics";
import {
  copyEvent,
  copyWhat,
  KEYBOARD_COPY_WINDOW_MS,
  QUICKSTART_ATTRIBUTE,
} from "@/lib/copy-analytics";

/**
 * Records every text copy on the site as a Vercel Analytics event, telling the
 * agent quickstart apart from code and other text, and Cmd/Ctrl+C apart from
 * the context menu. Copy buttons that write the clipboard directly track
 * themselves.
 */
export function CopyTracker() {
  useEffect(() => {
    let lastShortcut = 0;
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "c") {
        lastShortcut = Date.now();
      }
    };
    const onCopy = () => {
      const selection = window.getSelection();
      const selected = selection?.toString().trim() ?? "";
      if (!selection || !selected) return;
      const node = selection.anchorNode;
      const element = node instanceof Element ? node : node?.parentElement;
      const quickstart = element?.closest(`[${QUICKSTART_ATTRIBUTE}]`);
      const what = copyWhat(
        { inQuickstart: Boolean(quickstart), inCode: Boolean(element?.closest("pre, code")) },
        selected.length,
        quickstart?.textContent?.trim().length ?? 0,
      );
      const method = Date.now() - lastShortcut < KEYBOARD_COPY_WINDOW_MS ? "keyboard" : "menu";
      const event = copyEvent(what, method);
      track(event.name, event.properties);
    };
    window.addEventListener("keydown", onKeyDown, true);
    document.addEventListener("copy", onCopy);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
      document.removeEventListener("copy", onCopy);
    };
  }, []);
  return null;
}
