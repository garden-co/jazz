"use client";

import { useCallback } from "react";
import { useToast } from "@astryxdesign/core";
import { PersistedWriteRejectedError } from "jazz-tools";

/**
 * Writes apply locally first, so dialogs close at once. This waits for the
 * server in the background and reports a refusal, for example when a role
 * lost a permission after the page rendered.
 */
export function useWrite() {
  const toast = useToast();
  return useCallback(
    (failure: string, write: () => Promise<unknown>) => {
      void write().catch((error: unknown) => {
        const reason =
          error instanceof PersistedWriteRejectedError
            ? "the server refused the change."
            : error instanceof Error
              ? error.message
              : String(error);
        toast({ type: "error", body: `${failure}: ${reason}` });
      });
    },
    [toast],
  );
}
