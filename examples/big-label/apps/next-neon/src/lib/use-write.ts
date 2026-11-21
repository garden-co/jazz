"use client";

import { useCallback } from "react";
import { useToast } from "@astryxdesign/core";
import type { Write } from "./live";

/**
 * Writes apply to the live queries at once (an optimistic overlay), so
 * dialogs close at once. This waits for the server in the background and
 * reports a refusal, for example when a role lost a permission after the
 * page rendered.
 */
export function useWrite() {
  const toast = useToast();
  return useCallback(
    (failure: string, write: Write) => {
      void write.server.then((result) => {
        if (result.type === "success") return;
        const { message } = result.error;
        const reason = message.startsWith("permission_denied") ? "the server refused the change." : message;
        toast({ type: "error", body: `${failure}: ${reason}` });
      });
    },
    [toast],
  );
}
