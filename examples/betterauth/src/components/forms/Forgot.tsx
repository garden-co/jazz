import { Button } from "@/components/Button";
import { Loading } from "@/components/Loading";
import { useAuth } from "jazz-react-auth-betterauth";
import type { FullAuthClient } from "jazz-react-auth-betterauth";
import { useState } from "react";

const title = "Forgot Password";

export default function ForgotForm() {
  const auth = useAuth();
  const [email, setEmail] = useState<string>("");
  const [otp, setOtp] = useState<string>("");
  const [otpSentStatus, setOtpSentStatus] = useState<boolean>(false);
  const [otpStatus, setOtpStatus] = useState<boolean>(false);
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [status, setStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<Error | undefined>(undefined);

  return (
    <div className="min-h-screen flex flex-col justify-center">
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        <div>
          <h1 className="text-stone-950 dark:text-white font-display text-5xl lg:text-6xl font-medium tracking-tighter mb-2">
            {title}
          </h1>
          <p>
            Enter your email address, and we'll send you a link to reset your
            password.
          </p>
        </div>

        {status && !otpStatus && (
          <div>
            Instructions to reset your password have been sent to {email}, if an
            account with that email address exists.
          </div>
        )}

        {otpStatus && (
          <div>
            Your password has been successfully reset. You may now log in.
          </div>
        )}

        {error && <div>{error.message}</div>}

        {loading && <Loading />}

        <form className="flex flex-col gap-6">
          <div>
            <label htmlFor="email-address">Email address</label>
            <input
              id="email-address"
              value={email}
              disabled={loading}
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>
          <Button
            onClick={async (e) => {
              e.preventDefault();
              setLoading(true);
              const { data, error } = await auth.authClient.forgetPassword({
                email: email,
                redirectTo: `${window.location.origin}/reset`,
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
            disabled={loading}
          >
            Send recovery link
          </Button>
          <Button
            onClick={async (e) => {
              e.preventDefault();
              setLoading(true);
              const { data, error } = await (
                auth.authClient as FullAuthClient
              ).emailOtp.sendVerificationOtp({
                email: email,
                type: "forget-password",
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
            disabled={loading}
          >
            Send recovery one-time password
          </Button>
        </form>

        {otpSentStatus && (
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
              const { data, error } = await (
                auth.authClient as FullAuthClient
              ).emailOtp.resetPassword({
                email: email,
                otp: otp,
                password: password,
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
            <div>
              <label htmlFor="otp">One-time password</label>
              <input
                id="otp"
                value={otp}
                disabled={loading}
                onChange={(e) => setOtp(e.target.value)}
              />
            </div>
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
            <Button type={"submit"} disabled={loading}>
              Submit
            </Button>
          </form>
        )}

        <Button href="/sign-in" disabled={loading}>
          Sign in
        </Button>
      </div>
    </div>
  );
}
