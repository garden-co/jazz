import { useEffect } from "react";

import { useJazzContext } from "jazz-react-core";
import { CoValue, CoValueClass, ID, parseInviteLink } from "jazz-tools";
import { Linking } from "react-native";
import { RegisteredAccount } from "./provider.js";

export {
  useCoState,
  experimental_useInboxSender,
  useDemoAuth,
  usePassphraseAuth,
  useJazzContext,
  useAuthSecretStorage,
  useIsAuthenticated,
  useAccount,
  useAccountOrGuest,
} from "jazz-react-core";

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
    const handleDeepLink = ({ url }: { url: string }) => {
      const result = parseInviteLink<V>(url);
      if (result && result.valueHint === forValueHint) {
        context.me
          .acceptInvite(
            result.valueID,
            result.inviteSecret,
            invitedObjectSchema,
          )
          .then(() => {
            onAccept(result.valueID);
          })
          .catch((e) => {
            console.error("Failed to accept invite", e);
          });
      }
    };

    const linkingListener = Linking.addEventListener("url", handleDeepLink);

    void Linking.getInitialURL().then((url) => {
      if (url) handleDeepLink({ url });
    });

    return () => {
      linkingListener.remove();
    };
  }, [context, onAccept, invitedObjectSchema, forValueHint]);
}
