import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import { useEffect } from "react";

import { CoValueClassOrSchema } from "jazz-tools";
import { useJazzContext } from "jazz-tools/react-core";

export { useCoState, useAuthSecretStorage } from "jazz-tools/react-core";

export function useAcceptInvite<S extends CoValueClassOrSchema>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: S;
  onAccept: (valueID: string) => void;
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

export {
  experimental_useInboxSender,
  useJazzContext,
  useAccount,
  useCoStateWithSelector,
  useAccountWithSelector,
  useSyncConnectionStatus,
  useCoValueSubscription,
  useAccountSubscription,
  useSubscriptionSelector,
} from "jazz-tools/react-core";
