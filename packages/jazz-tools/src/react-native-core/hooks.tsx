import { useEffect } from "react";

import { CoValueClassOrSchema, parseInviteLink } from "jazz-tools";
import { useJazzContext } from "jazz-tools/react-core";
import { Linking } from "react-native";

export {
  useCoState,
  experimental_useInboxSender,
  useDemoAuth,
  usePassphraseAuth,
  useJazzContext,
  useAuthSecretStorage,
  useIsAuthenticated,
  useAccount,
  useCoStateWithSelector,
  useAccountWithSelector,
  useSyncConnectionStatus,
  useCoValueSubscription,
  useAccountSubscription,
  useSubscriptionSelector,
} from "jazz-tools/react-core";

export function useAcceptInviteNative<S extends CoValueClassOrSchema>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: S;
  onAccept: (projectID: string) => void;
  forValueHint?: string;
}): void {
  const context = useJazzContext();

  if (!("me" in context)) {
    throw new Error(
      "useAcceptInviteNative can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  useEffect(() => {
    const handleDeepLink = ({ url }: { url: string }) => {
      const result = parseInviteLink(url);
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
