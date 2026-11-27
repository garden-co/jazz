"use client";

import { useCallback } from "react";
import { useToast } from "@astryxdesign/core";
import { ConvexError } from "convex/values";

/**
 * Runs a write and reports a refusal, for example when a role lost a
 * permission after the page rendered.
 */
export function useWrite() {
  const toast = useToast();
  return useCallback(
    (failure: string, write: () => Promise<unknown>) => {
      void write().catch((error: unknown) => {
        const reason =
          error instanceof ConvexError
            ? error.data === "permission_denied"
              ? "the server refused the change."
              : String(error.data)
            : error instanceof Error
              ? error.message
              : String(error);
        toast({ type: "error", body: `${failure}: ${reason}` });
      });
    },
    [toast],
  );
}
