import { Button } from "@/components/Button";
import { Loading } from "@/components/Loading";
import { SSOButton } from "@/components/SSOButton";
import { useAuth } from "jazz-react-auth-betterauth";
import type {
  FullAuthClient,
  SSOProviderType,
} from "jazz-react-auth-betterauth";
import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";

const title = "Sign Up";

export default function SignUpForm({
  providers,
}: {
  providers?: SSOProviderType[];
}) {
  const router = useRouter();
  const auth = useAuth();
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
          <div>A one-time password has been sent to your email.</div>
        )}

        {error && <div>{error.message}</div>}

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
                    router.push("/");
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
                router.push("/");
              }
            }
            setLoading(false);
          }}
        >
          <div>
            <label htmlFor="full-name">Full name</label>
            <input
              id="full-name"
              value={name}
              disabled={loading}
              onChange={(e) => setName(e.target.value)}
            />
          </div>
          <div>
            <label htmlFor="email-address">Email address</label>
            <input
              id="email-address"
              value={email}
              disabled={loading}
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>
          {!otpStatus && (
            <>
              <div>
                <label htmlFor="password">Password</label>
                <input
                  id="password"
                  type="password"
                  value={password}
                  disabled={loading}
                  onChange={(e) => setPassword(e.target.value)}
                />
              </div>
              <div>
                <label htmlFor="confirm-password">Confirm password</label>
                <input
                  id="confirm-password"
                  type="password"
                  value={confirmPassword}
                  disabled={loading}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                />
              </div>
            </>
          )}
          {otpStatus && (
            <div>
              <label htmlFor="otp">One-time password</label>
              <input
                id="otp"
                value={otp}
                disabled={loading}
                onChange={(e) => setOtp(e.target.value)}
              />
            </div>
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
