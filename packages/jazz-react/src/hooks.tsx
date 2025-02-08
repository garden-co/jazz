"use client";

import { consumeInviteLinkFromWindowLocation } from "jazz-browser";
import { useEffect } from "react";

import { createUseAccountHooks, useJazzContext } from "jazz-react-core";
import { CoValue, CoValueClass, ID } from "jazz-tools";
import { RegisteredAccount } from "./provider.js";

export {
  useCoState,
  useHydratedCoState,
} from "jazz-react-core";

export const { useAccount, useAccountOrGuest } =
  createUseAccountHooks<RegisteredAccount>();

export function useAcceptInvite<V extends CoValue>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: CoValueClass<V>;
  onAccept: (projectID: ID<V>) => void;
  forValueHint?: string;
}): void {
  const context = useJazzContext();

  if (!("me" in context)) {
    throw new Error(
      "useAcceptInvite can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  useEffect(() => {
    const handleInvite = () => {
      const result = consumeInviteLinkFromWindowLocation({
        as: context.me,
        invitedObjectSchema,
        forValueHint,
      });

      result
        .then((result) => result && onAccept(result?.valueID))
        .catch((e) => {
          console.error("Failed to accept invite", e);
        });
    };

    handleInvite();

    window.addEventListener("hashchange", handleInvite);

    return () => window.removeEventListener("hashchange", handleInvite);
  }, [onAccept]);
}

export { experimental_useInboxSender } from "jazz-react-core";
