import { Alert } from "@garden-co/design-system/src/components/atoms/Alert";
import { Heading } from "@garden-co/design-system/src/components/atoms/Heading";
import { Input } from "@garden-co/design-system/src/components/molecules/Input";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useCallback, useEffect, useState } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import type { FullAuthClient } from "../../types/auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { AccountProviders } from "../AccountProviders";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { DeleteAccountButton } from "../DeleteAccountButton";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { SSOButton } from "../SSOButton";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "../common/Button";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Loading } from "../common/Loading";

const title = "Settings";

declare const listAccounts: ReturnType<
  typeof useAuth
>["auth"]["authClient"]["listAccounts"];
type AccountsType = Awaited<ReturnType<typeof listAccounts<{}>>>;

export default function SettingsForm({
  providers,
}: {
  providers?: Parameters<
    ReturnType<typeof useAuth>["auth"]["authClient"]["signIn"]["social"]
  >[0]["provider"][];
}) {
  const { auth, account, navigate, user } = useAuth();

  const [accounts, setAccounts] = useState<AccountsType | undefined>(undefined);
  useEffect(() => {
    auth.authClient.useSession.subscribe(() => {
      auth.authClient.listAccounts().then((x) => setAccounts(x));
    });
  }, [user, account, accounts]);

  const [status, setStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [otpSentStatus, setOtpSentStatus] = useState<boolean>(false);
  const [otpStatus, setOtpStatus] = useState<boolean>(false);
  const [otp, setOtp] = useState<string>("");

  const { me, logOut } = useAccount({ resolve: { profile: true } });
  const isAuthenticated = useIsAuthenticated();
  const signOut = useCallback(() => {
    auth.authClient.signOut({
      fetchOptions: {
        onSuccess: () => {
          logOut();
          navigate("/");
        },
      },
    });
  }, [logOut, auth]);

  return (
    <>
      <header className="absolute p-4 top-0 left-0 w-full z-10 flex items-center justify-between gap-4">
        <div className="float-start">
          {me && user && account && isAuthenticated && (
            <Button className="float-start" onClick={signOut}>
              Sign out
            </Button>
          )}
        </div>
      </header>
      <div className="min-h-screen flex flex-col justify-center font-[family-name:var(--font-geist-sans)]">
        <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
          <Heading level={1} className="mb-2">
            {title}
          </Heading>

          {status && account && !account?.emailVerified && (
            <Alert variant="info" title="Settings">
              Instructions to verify your account have been sent to{" "}
              {account.email}, if an account with that email address exists.
            </Alert>
          )}

          {(status || otpStatus) && account && account.emailVerified && (
            <Alert variant="info" title="Settings">
              Your account has been successfully verified.
            </Alert>
          )}

          {error && (
            <Alert variant="warning" title="Settings">
              {error.message}
            </Alert>
          )}

          {loading && <Loading />}

          <AccountProviders
            accounts={accounts}
            setLoading={setLoading}
            setError={setError}
          />
          {accounts?.data &&
            providers?.map((x) => {
              return (
                accounts.data.find((y) => y.provider === x) === undefined && (
                  <SSOButton
                    link={true}
                    provider={x}
                    setLoading={setLoading}
                    setError={setError}
                  />
                )
              );
            })}
          {account && account.emailVerified && <p>Account verified.</p>}
          {account && !account.emailVerified && (
            <>
              <Button
                variant="secondary"
                className="relative"
                onClick={async (e) => {
                  e.preventDefault();
                  setLoading(true);
                  const { data, error } =
                    await auth.authClient.sendVerificationEmail({
                      email: account.email,
                      callbackURL: `${window.location.origin}`,
                    });
                  setStatus(data?.status ?? false);
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
                Send verification link
              </Button>
              <Button
                variant="secondary"
                className="relative"
                onClick={async (e) => {
                  e.preventDefault();
                  setLoading(true);
                  const { data, error } = await (
                    auth.authClient as FullAuthClient
                  ).emailOtp.sendVerificationOtp({
                    email: account.email,
                    type: "email-verification",
                  });
                  setStatus(data?.success ?? false);
                  setOtpSentStatus(data?.success ?? false);
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
                Send verification one-time password
              </Button>
            </>
          )}
          {otpSentStatus && account && !account.emailVerified && (
            <form
              className="flex flex-col gap-6"
              onSubmit={async (e) => {
                e.preventDefault();
                setLoading(true);
                const { data, error } = await (
                  auth.authClient as FullAuthClient
                ).emailOtp.verifyEmail({
                  email: account.email,
                  otp: otp,
                });
                setOtpStatus(data?.status ?? false);
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
              <Input
                label="One-time password"
                value={otp}
                disabled={loading}
                onChange={(e) => setOtp(e.target.value)}
              />
              <Button type={"submit"} disabled={loading}>
                Submit
              </Button>
            </form>
          )}
          <DeleteAccountButton
            setLoading={setLoading}
            setError={setError}
            callbackURL={`${window.location.origin}/delete-account`}
          />
        </div>
      </div>
    </>
  );
}
