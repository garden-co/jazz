import { useEffect } from "react";

import { useJazzContext } from "jazz-react-core";
import { CoValueOrZodSchema, parseInviteLink } from "jazz-tools";
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
} from "jazz-react-core";

export function useAcceptInvite<S extends CoValueOrZodSchema>({
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
      "useAcceptInvite can't be used in a JazzProvider with auth === 'guest'.",
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
