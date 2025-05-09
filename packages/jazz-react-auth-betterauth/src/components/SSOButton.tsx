import { forwardRef } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { socialProviderNames } from "../lib/social";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "./common/Button";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children?: React.ReactNode;
  src?: InstanceType<typeof Image>["src"];
  alt?: InstanceType<typeof Image>["alt"];
  provider: Parameters<
    ReturnType<typeof useAuth>["auth"]["authClient"]["signIn"]["social"]
  >[0]["provider"];
  link?: boolean;
  callbackURL?: string;
  setLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setError: React.Dispatch<React.SetStateAction<Error | undefined>>;
}

export const SSOButton = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      children,
      provider,
      link = false,
      callbackURL,
      setLoading,
      setError,
      ...buttonProps
    },
    ref,
  ) => {
    const { auth } = useAuth();
    const providerName = socialProviderNames[provider];
    return (
      <Button
        src={`/social/${provider}.svg`}
        alt={`${providerName} logo`}
        imageClassName="absolute left-3 dark:invert"
        variant="secondary"
        className="relative"
        onClick={async (e) => {
          e.preventDefault();
          setLoading(true);
          const { error } = await (async () => {
            if (link) {
              return await auth.authClient.linkSocial({
                provider: provider,
              });
            } else {
              return await auth.authClient.signIn.social({
                provider: provider,
                callbackURL: callbackURL,
              });
            }
          })();
          if (error) {
            setError({
              ...error,
              name: error.message ?? error.statusText,
              message: error.message ?? error.statusText,
            });
          }
          setLoading(false);
        }}
        {...buttonProps}
        ref={ref}
      >
        {link
          ? `Link ${providerName} account`
          : `Continue with ${providerName}`}
        {children}
      </Button>
    );
  },
);
