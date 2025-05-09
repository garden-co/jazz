import { Alert } from "@garden-co/design-system/src/components/atoms/Alert";
import { Heading } from "@garden-co/design-system/src/components/atoms/Heading";
import { Input } from "@garden-co/design-system/src/components/molecules/Input";
import { useState } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import type { FullAuthClient } from "../../types/auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "../common/Button";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Loading } from "../common/Loading";

const title = "Forgot Password";

export default function ForgotForm() {
  const { auth, Image, Link } = useAuth();
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
          <Heading level={1} className="mb-2">
            {title}
          </Heading>
          <p>
            Enter your email address, and we'll send you a link to reset your
            password.
          </p>
        </div>

        {status && !otpStatus && (
          <Alert variant="info" title={title}>
            Instructions to reset your password have been sent to {email}, if an
            account with that email address exists.
          </Alert>
        )}

        {otpStatus && (
          <Alert variant="info" title={title}>
            Your password has been successfully reset. You may now log in.
          </Alert>
        )}

        {error && (
          <Alert variant="warning" title={title}>
            {error.message}
          </Alert>
        )}

        {loading && <Loading />}

        <form className="flex flex-col gap-6">
          <Input
            label="Email address"
            value={email}
            disabled={loading}
            onChange={(e) => setEmail(e.target.value)}
          />
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
            <Input
              label="One-time password"
              value={otp}
              disabled={loading}
              onChange={(e) => setOtp(e.target.value)}
            />
            <Input
              label="Password"
              value={password}
              type="password"
              disabled={loading}
              onChange={(e) => setPassword(e.target.value)}
            />
            <Input
              label="Confirm password"
              value={confirmPassword}
              type="password"
              disabled={loading}
              onChange={(e) => setConfirmPassword(e.target.value)}
            />
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
