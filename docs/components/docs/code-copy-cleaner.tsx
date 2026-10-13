"use client";

import { useEffect } from "react";

const ZERO_WIDTH_SPACE = /​/g;

/**
 * Astryx CodeBlock renders each blank line as a zero-width space so the line
 * keeps its height. Selecting code and copying it would put those characters
 * on the clipboard, and pasted JS/TS then fails to parse. This strips them
 * from any copy whose selection touches a code block; the Copy button already
 * copies the raw source.
 */
export function CodeCopyCleaner() {
  useEffect(() => {
    const onCopy = (event: ClipboardEvent) => {
      const selection = document.getSelection();
      if (!selection || selection.isCollapsed || !event.clipboardData) return;
      const text = selection.toString();
      if (!text.includes("\u200B")) return;
      const range = selection.getRangeAt(0);
      const container =
        range.commonAncestorContainer instanceof Element
          ? range.commonAncestorContainer
          : range.commonAncestorContainer.parentElement;
      const inCode =
        container?.closest(".astryx-code-block") ?? container?.querySelector(".astryx-code-block");
      if (!inCode) return;
      event.clipboardData.setData("text/plain", text.replace(ZERO_WIDTH_SPACE, ""));
      event.preventDefault();
    };
    document.addEventListener("copy", onCopy);
    return () => document.removeEventListener("copy", onCopy);
  }, []);
  return null;
}
