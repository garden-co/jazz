"use client";

import { Button } from "@/components/Button";
import { Loading } from "@/components/Loading";
import { useAuth } from "@/contexts/Auth";
import { Alert } from "@garden-co/design-system/design-system/src/components/atoms/Alert";
import { TextLink } from "@garden-co/design-system/design-system/src/components/atoms/TextLink";
import { Input } from "@garden-co/design-system/design-system/src/components/molecules/Input";
import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";

const title = "Sign up";

export default function Page() {
  const router = useRouter();
  const { auth, user, account } = useAuth();
  const [name, setName] = useState<string>("");
  const [email, setEmail] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [status, setStatus] = useState<boolean>(false);
  const [otp, setOtp] = useState<string>("");
  const [otpStatus, setOtpStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | undefined>(undefined);

  const ssoSignUp =
    (
      provider: Parameters<typeof auth.authClient.signIn.social>[0]["provider"],
    ) =>
    async (e: React.MouseEvent<HTMLButtonElement>) => {
      e.preventDefault();
      setLoading(true);
      const { error } = await auth.authClient.signIn.social({
        provider: provider,
        callbackURL: `${window.location.origin}/social/signIn`,
      });
      if (error) {
        setError({
          ...error,
          name: error.message ?? error.statusText,
          message: error.message ?? error.statusText,
        });
      }
      setLoading(false);
    };

  return (
    <div className="min-h-screen flex flex-col justify-center">
      <h1 className="sr-only">{title}</h1>
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        <Link href="/">
          <span className="sr-only">Back to home</span>
          <Image
            src="/jazz-logo.svg"
            alt="Jazz logo"
            width={180}
            height={38}
            priority
          />
        </Link>

        {otpStatus && (
          <Alert variant="info" title="Sign Up">
            A one-time password has been sent to your email.
          </Alert>
        )}

        {error && (
          <Alert variant="warning" title="Sign Up">
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
                    router.push("/");
                  },
                  onError: (error) => {
                    setError(error.error);
                  },
                },
              );
            } else {
              const { data, error } = await auth.authClient.signIn.emailOtp({
                email: email,
                otp: otp,
              });
              setStatus(data !== null);
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
          <Button
            variant="secondary"
            className="relative"
            onClick={ssoSignUp("github")}
          >
            <Image
              src="/social/github.svg"
              alt="GitHub logo"
              className="absolute left-3 dark:invert"
              width={16}
              height={16}
              priority
            />
            Continue with GitHub
          </Button>
          <Button
            variant="secondary"
            className="relative"
            onClick={async (e) => {
              e.preventDefault();
              setLoading(true);
              const { data, error } = await auth.authClient.signIn.magicLink({
                email: email,
                callbackURL: `${window.location.origin}/magic-link/signIn`,
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
            <Image
              src="/link.svg"
              alt="Link icon"
              className="absolute left-3"
              width={16}
              height={16}
              priority
            />
            Sign up with magic link
          </Button>
          <Button
            variant="secondary"
            className="relative"
            onClick={async (e) => {
              e.preventDefault();
              setLoading(true);
              const { data, error } =
                await auth.authClient.emailOtp.sendVerificationOtp({
                  email: email,
                  type: "sign-in",
                });
              setStatus(data?.success ?? false);
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
              priority
            />
            Sign up with one-time password
          </Button>
        </div>

        <p className="text-sm">
          Already have an account? <TextLink href="/sign-in">Sign in</TextLink>
        </p>
      </div>
    </div>
  );
}
