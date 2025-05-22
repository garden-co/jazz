import { consumeInviteLinkFromWindowLocation } from "jazz-browser";
import { useEffect } from "react";

import { useJazzContext } from "jazz-react-core";
import { CoValueOrZodSchema } from "jazz-tools";

export { useCoState, useAuthSecretStorage } from "jazz-react-core";

export function useAcceptInvite<S extends CoValueOrZodSchema>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: S;
  onAccept: (valueID: string) => void;
  forValueHint?: string;
}): void {
  const context = useJazzContext();

  useEffect(() => {
    if (!("me" in context)) {
      return;
    }

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

export {
  experimental_useInboxSender,
  useJazzContext,
  useAccount,
} from "jazz-react-core";
