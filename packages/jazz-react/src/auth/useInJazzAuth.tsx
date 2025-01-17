import { JazzContext } from "jazz-react-core";
import { AuthMethod } from "jazz-tools";
import { useContext, useEffect } from "react";
import { useAccount } from "../hooks.js";

export type AuthChangeProps = {
  username: string;
  isSignUp: boolean;
  isLogIn: boolean;
};

export function useInJazzAuth({
  auth,
  onAuthChange,
}: {
  auth: AuthMethod;
  onAuthChange: (props: AuthChangeProps) => void;
}) {
  const { me } = useAccount();
  const context = useContext(JazzContext);

  useEffect(() => {
    if (!context) return;

    const runAuth = async () => {
      const result = await auth.start(me._raw.core.node.crypto);

      if (result.type === "new") {
        throw new Error(
          "New credentials generation is not supported in this context, yet",
        );
      }

      try {
        await result.saveCredentials?.({
          accountID: result.credentials.accountID,
          secret: result.credentials.secret,
        });
      } catch (error) {
        result.onError(error as string | Error);
        return;
      }

      result.onSuccess();

      const isSignUp = result.credentials.accountID === me.id;

      if (!isSignUp) {
        context.refreshContext?.();
      }

      onAuthChange({
        username: result.username ?? "",
        isSignUp,
        isLogIn: !isSignUp,
      });
    };

    runAuth();
  }, [auth]);
}
