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

const title = "Sign Up";

export default function SignUpForm({
  providers,
}: {
  providers?: Parameters<
    ReturnType<typeof useAuth>["auth"]["authClient"]["signIn"]["social"]
  >[0]["provider"][];
}) {
  const { auth, Image, Link, navigate } = useAuth();
  const [name, setName] = useState<string>("");
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
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
            if (password !== confirmPassword) {
              setError(new Error("Passwords do not match"));
              setLoading(false);
              return;
            }
            if (!otpStatus) {
              await auth.authClient.signUp.email(
                {
                  email,
                  password,
                  name,
                },
                {
                  onSuccess: async () => {
                    await auth.signIn();
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
                await auth.signIn();
                navigate("/");
              }
            }
            setLoading(false);
          }}
        >
          <Input
            label="Full name"
            disabled={loading}
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
          <Input
            label="Email address"
            disabled={loading}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          {!otpStatus && (
            <>
              <Input
                label="Password"
                type="password"
                disabled={loading}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
              <Input
                label="Confirm password"
                type="password"
                disabled={loading}
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
            </>
          )}
          {otpStatus && (
            <Input
              label="One-time password"
              disabled={loading}
              value={otp}
              onChange={(e) => setOtp(e.target.value)}
            />
          )}
          <Button type="submit" disabled={loading}>
            Sign up
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
                callbackURL={`${window.location.origin}/social/signIn`}
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
                callbackURL: `${window.location.origin}/magic-link/signIn`,
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
            Sign up with magic link
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
            Sign up with one-time password
          </Button>
        </div>

        <p className="text-sm">
          Already have an account? <Link href="/sign-in">Sign in</Link>
        </p>
      </div>
    </div>
  );
}
