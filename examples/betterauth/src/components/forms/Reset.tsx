import { Button } from "@/components/Button";
import { Loading } from "@/components/Loading";
import { useAuth } from "jazz-react-auth-betterauth";
import Link from "next/link";
import { useState } from "react";

const title = "Reset Password";

export default function ResetForm() {
  const auth = useAuth();
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
          <h1 className="text-stone-950 dark:text-white font-display text-5xl lg:text-6xl font-medium tracking-tighter mb-2">
            {title}
          </h1>
          <p>Enter your new password.</p>
        </div>

        {status && (
          <div>
            Your password has been reset. You may now{" "}
            <Link href="/sign-in">sign in</Link>.
          </div>
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
