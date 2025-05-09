import { forwardRef } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "./common/Button";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children?: React.ReactNode;
  src?: InstanceType<typeof Image>["src"];
  alt?: InstanceType<typeof Image>["alt"];
  callbackURL: string;
  setLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setError: React.Dispatch<React.SetStateAction<Error | undefined>>;
}

export const DeleteAccountButton = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ children, callbackURL, setLoading, setError, ...buttonProps }, ref) => {
    const { auth, replace } = useAuth();
    return (
      <Button
        variant="danger"
        className="relative"
        onClick={async (e) => {
          e.preventDefault();
          setLoading(true);
          const { error } = await auth.authClient.deleteUser(
            {
              callbackURL: callbackURL,
            },
            {
              onSuccess: () => {
                replace(callbackURL);
              },
            },
          );
          const errorMessage = error?.message ?? error?.statusText;
          setError(
            error
              ? {
                  ...error,
                  name: error.statusText,
                  message:
                    errorMessage && errorMessage.length > 0
                      ? errorMessage
                      : "An error occurred",
                }
              : undefined,
          );
          setLoading(false);
        }}
      >
        Delete account
      </Button>
    );
  },
);
