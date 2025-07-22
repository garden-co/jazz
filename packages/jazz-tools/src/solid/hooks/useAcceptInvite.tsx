import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import { Accessor, createEffect, onCleanup } from "solid-js";

import { CoValueOrZodSchema } from "jazz-tools";
import { useJazzContext } from "../context/jazz.js";

type AcceptInviteParams<S extends CoValueOrZodSchema> = {
  readonly invitedObjectSchema: S;
  readonly forValueHint?: string;
  readonly onAccept: (valueID: string) => void;
};

export function useAcceptInvite<S extends CoValueOrZodSchema>(
  params: Accessor<AcceptInviteParams<S>>,
): void {
  const jazz = useJazzContext();

  const account = () => {
    const _jazz = jazz();

    if (!("me" in _jazz)) {
      throw new Error(
        "useAcceptInvite can't be used in a JazzProvider with auth === 'guest'.",
      );
    }

    return _jazz.me;
  };

  createEffect(() => {
    const { invitedObjectSchema, forValueHint, onAccept } = params();

    const handleInvite = async () => {
      try {
        const result = await consumeInviteLinkFromWindowLocation({
          as: account(),
          invitedObjectSchema,
          forValueHint,
        });

        if (result) onAccept(result.valueID);
      } catch (e) {
        console.error("Failed to accept invite", e);
      }
    };

    void handleInvite();

    window.addEventListener("hashchange", handleInvite);

    onCleanup(() => {
      window.removeEventListener("hashchange", handleInvite);
    });
  });
}
