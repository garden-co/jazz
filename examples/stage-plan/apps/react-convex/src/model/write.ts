import { useMutation } from "convex/react";
import { ConvexError } from "convex/values";
import type { FunctionReference } from "convex/server";
import { useToast } from "@astryxdesign/core";

/**
 * A mutation that tells you when the server didn't apply your change, like
 * the Jazz app's rejected-write toasts.
 */
export function useWrite<M extends FunctionReference<"mutation">>(mutation: M) {
  const run = useMutation(mutation);
  const toast = useToast();
  return (...args: Parameters<typeof run>) =>
    run(...args).catch((error: unknown) => {
      const body =
        error instanceof ConvexError && error.data === "permission_denied"
          ? "A change was undone because the server didn't allow it."
          : "A change couldn't be saved and was undone.";
      toast({ type: "error", body });
    });
}
