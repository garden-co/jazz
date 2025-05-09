import { Alert } from "@garden-co/design-system/src/components/atoms/Alert";
import { Heading } from "@garden-co/design-system/src/components/atoms/Heading";
import { Input } from "@garden-co/design-system/src/components/molecules/Input";
import { useState } from "react";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { useAuth } from "../../contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Button } from "../common/Button";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
import { Loading } from "../common/Loading";

const title = "Reset Password";

export default function ResetForm() {
  const { auth, Image, Link } = useAuth();
  const searchParams = new URLSearchParams(window.location.search);
  const token = searchParams.get("token");
  const initialError = searchParams.get("error");
  const [error, setError] = useState<Error | undefined>(
    initialError
      ? {
          name: initialError,
          message: initialError,
        }
      : undefined,
  );
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [status, setStatus] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(false);

  return (
    <div className="min-h-screen flex flex-col justify-center">
      <div className="max-w-md flex flex-col gap-8 w-full px-6 py-12 mx-auto">
        <div>
          <Heading level={1} className="mb-2">
            {title}
          </Heading>
          <p>Enter your new password.</p>
        </div>

        {status && (
          <Alert variant="info" title={title}>
            Your password has been reset. You may now{" "}
            <Link href="/sign-in">sign in</Link>.
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
            if (!token) {
              setError(new Error("No password reset token provided"));
              setLoading(false);
              return;
            }
            const { data, error } = await auth.authClient.resetPassword({
              newPassword: password,
              token,
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
          <Button type="submit" disabled={loading}>
            Reset password
          </Button>
        </form>

        <Button href="/sign-in" disabled={loading}>
          Sign in
        </Button>
      </div>
    </div>
  );
}
