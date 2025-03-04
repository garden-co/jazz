import { consumeInviteLinkFromWindowLocation } from "jazz-browser";
import { useEffect } from "react";

import { useJazzContext } from "jazz-react-core";
import { CoValue, CoValueClass, ID } from "jazz-tools";
import { RegisteredAccount } from "./provider.js";

export { useCoState, useAuthSecretStorage } from "jazz-react-core";

declare module "jazz-react-core" {
  export interface Register {
    Account: RegisteredAccount;
  }
}

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

export {
  experimental_useInboxSender,
  useJazzContext,
  useAccount,
  useAccountOrGuest,
} from "jazz-react-core";
