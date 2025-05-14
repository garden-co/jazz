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

const title = "Sign In";

export default function SignInForm({
  providers,
}: {
  providers?: SSOProviderType[];
}) {
  const router = useRouter();
  const auth = useAuth();
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
          <div>A one-time password has been sent to your email.</div>
        )}

        {error && <div>{error.message}</div>}

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
                await auth.logIn();
                router.push("/");
              }
            }
            setLoading(false);
          }}
        >
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
          <div className="items-center">
            <div>
              <label htmlFor="remember-me">Remember me</label>
              <input
                id="remember-me"
                type="checkbox"
                checked={rememberMe}
                disabled={loading}
                onChange={(e) => setRememberMe(e.target.checked)}
              />
            </div>
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
