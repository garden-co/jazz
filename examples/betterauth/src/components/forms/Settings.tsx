import { AccountProviders } from "@/components/AccountProviders";
import { Button } from "@/components/Button";
import { DeleteAccountButton } from "@/components/DeleteAccountButton";
import { Loading } from "@/components/Loading";
import { SSOButton } from "@/components/SSOButton";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useAuth } from "jazz-react-auth-betterauth";
import type {
  AccountsType,
  FullAuthClient,
  SSOProviderType,
} from "jazz-react-auth-betterauth";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";

const title = "Settings";

export default function SettingsForm({
  providers,
}: {
  providers?: SSOProviderType[];
}) {
  const router = useRouter();
  const { authClient, account, state } = useAuth();
  const hasCredentials = state !== "anonymous";

  const [accounts, setAccounts] = useState<AccountsType | undefined>(undefined);
  useEffect(() => {
    return authClient.useSession.subscribe(() => {
      authClient.listAccounts().then((x) => setAccounts(x));
    });
  }, [authClient]);

  const [status, setStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [otpSentStatus, setOtpSentStatus] = useState<boolean>(false);
  const [otpStatus, setOtpStatus] = useState<boolean>(false);
  const [otp, setOtp] = useState<string>("");

  const { me, logOut } = useAccount({ resolve: { profile: true } });
  const isAuthenticated = useIsAuthenticated();
  const signOut = useCallback(() => {
    authClient
      .signOut()
      .catch(console.error)
      .finally(() => {
        logOut();
        router.push("/");
      });
  }, [logOut, authClient]);

  return (
    <>
      <header className="absolute p-4 top-0 left-0 w-full z-10 flex items-center justify-between gap-4">
        <div className="float-start">
          {me && hasCredentials && account && isAuthenticated && (
            <Button className="float-start" onClick={signOut}>
              Sign out
            </Button>
          )}
        </div>
      </header>
      <div className="min-h-screen flex flex-col justify-center font-[family-name:var(--font-geist-sans)]">
        <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
          <h1 className="text-stone-950 dark:text-white font-display text-5xl lg:text-6xl font-medium tracking-tighter mb-2">
            {title}
          </h1>

          {status && account && !account?.emailVerified && (
            <div>
              Instructions to verify your account have been sent to{" "}
              {account.email}, if an account with that email address exists.
            </div>
          )}

          {(status || otpStatus) && account && account.emailVerified && (
            <div>Your account has been successfully verified.</div>
          )}

          {error && <div>{error.message}</div>}

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
                    await authClient.sendVerificationEmail({
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
                    authClient as FullAuthClient
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
                  authClient as FullAuthClient
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
              <div>
                <label htmlFor="otp">One-time password</label>
                <input
                  id="otp"
                  value={otp}
                  disabled={loading}
                  onChange={(e) => setOtp(e.target.value)}
                />
              </div>
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
