import { Alert } from "@garden-co/design-system/src/components/atoms/Alert";
import { Input } from "@garden-co/design-system/src/components/molecules/Input";
import { useState } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import type { FullAuthClient } from "../../types/auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { SSOButton } from "../SSOButton";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "../common/Button";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Loading } from "../common/Loading";

const title = "Sign In";

export default function SignInForm({
  providers,
}: {
  providers?: Parameters<
    ReturnType<typeof useAuth>["auth"]["authClient"]["signIn"]["social"]
  >[0]["provider"][];
}) {
  const { auth, Image, Link, navigate } = useAuth();
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [rememberMe, setRememberMe] = useState(true);
  const [otp, setOtp] = useState<string>("");
  const [otpStatus, setOtpStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | undefined>(undefined);

  return (
    <div className="min-h-screen flex flex-col justify-center">
      <h1 className="sr-only">{title}</h1>
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        {otpStatus && (
          <Alert variant="info" title={title}>
            A one-time password has been sent to your email.
          </Alert>
        )}

        {error && (
          <Alert variant="warning" title={title}>
            {error.message}
          </Alert>
        )}

        {loading && <Loading />}

        <form
          className="flex flex-col gap-6"
          onSubmit={async (e) => {
            e.preventDefault();
            setLoading(true);
            if (!otpStatus) {
              await auth.authClient.signIn.email(
                {
                  email,
                  password,
                  rememberMe,
                },
                {
                  onSuccess: async () => {
                    await auth.logIn();
                    navigate("/");
                  },
                  onError: (error) => {
                    setError(error.error);
                  },
                },
              );
            } else {
              const { data, error } = await (
                auth.authClient as FullAuthClient
              ).signIn.emailOtp({
                email: email,
                otp: otp,
              });
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
              if (data) {
                await auth.logIn();
                navigate("/");
              }
            }
            setLoading(false);
          }}
        >
          <Input
            label="Email address"
            disabled={loading}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          {!otpStatus && (
            <Input
              label="Password"
              type="password"
              disabled={loading}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          )}
          {otpStatus && (
            <Input
              label="One-time password"
              disabled={loading}
              value={otp}
              onChange={(e) => setOtp(e.target.value)}
            />
          )}
          <div className="items-center">
            <Input
              label="Remember me"
              type="checkbox"
              className="text-sm truncate float-left gap-4 flex"
              checked={rememberMe}
              onChange={(e) => setRememberMe(e.target.checked)}
            />
            <Link href="/forgot" className="text-sm float-right">
              Forgot password?
            </Link>
          </div>
          <Button type="submit" disabled={loading}>
            Sign in
          </Button>
        </form>

        <div className="flex items-center gap-4">
          <hr className="flex-1" />
          <p className="text-center">or</p>
          <hr className="flex-1" />
        </div>

        <div className="flex flex-col gap-4">
          {providers?.map((x) => {
            return (
              <SSOButton
                callbackURL={`${window.location.origin}/social/logIn`}
                provider={x}
                setLoading={setLoading}
                setError={setError}
              />
            );
          })}
          <Button
            variant="secondary"
            className="relative"
            onClick={async (e) => {
              e.preventDefault();
              setLoading(true);
              const { error } = await (
                auth.authClient as FullAuthClient
              ).signIn.magicLink({
                email: email,
                callbackURL: `${window.location.origin}/magic-link/logIn`,
              });
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
            <Image
              src="/link.svg"
              alt="Link icon"
              className="absolute left-3"
              width={16}
              height={16}
            />
            Sign in with magic link
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
                email: email,
                type: "sign-in",
              });
              setOtpStatus(data?.success ?? false);
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
            <Image
              src="/mail.svg"
              alt="Mail icon"
              className="absolute left-3"
              width={16}
              height={16}
            />
            Sign in with one-time password
          </Button>
        </div>

        <p className="text-sm">
          Don't have an account? <Link href="/sign-up">Sign up</Link>
        </p>
      </div>
    </div>
  );
}
