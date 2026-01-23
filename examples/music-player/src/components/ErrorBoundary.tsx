import { MusicaAccount } from "@/1_schema";
import React from "react";
import { useAccount, useLogOut } from "jazz-tools/react";
import { getJazzErrorType } from "jazz-tools";

function ErrorUI({
  error,
  errorType,
}: {
  error: Error;
  errorType: ReturnType<typeof getJazzErrorType>;
}) {
  const logOut = useLogOut();
  const me = useAccount(MusicaAccount, { resolve: { root: true } });

  if (me.$jazz.loadingState === "deleted") {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            Your account data has been deleted
          </h1>
          <p className="text-muted-foreground">
            The account data associated with your session no longer exists.
            Please log out and sign in again to continue.
          </p>
          <button
            onClick={logOut}
            className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
          >
            Log out
          </button>
        </div>
      </div>
    );
  }

  if (errorType === "unauthorized") {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            You are not authorized to access this page
          </h1>
          <button
            onClick={() => {
              window.location.href = "/";
            }}
            className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
          >
            Go to home page
          </button>
        </div>
      </div>
    );
  }

  if (errorType === "deleted") {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The page you are trying to access has been deleted
          </h1>
          <button
            onClick={() => {
              window.location.href = "/";
            }}
            className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
          >
            Go to home page
          </button>
        </div>
      </div>
    );
  }

  if (errorType === "unavailable") {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The page you are trying to access is unavailable
          </h1>
          <button
            onClick={() => {
              window.location.href = "/";
            }}
            className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
          >
            Go to home page
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen items-center justify-center p-8">
      <div className="max-w-2xl space-y-4">
        <h1 className="text-2xl font-semibold text-red-600">
          Something went wrong
        </h1>
        <p className="text-muted-foreground">
          {error.message || "An unexpected error occurred"}
        </p>
        {process.env.NODE_ENV === "development" && (
          <pre className="mt-4 overflow-auto rounded-md bg-muted p-4 text-sm">
            {error.stack}
          </pre>
        )}
        <button
          onClick={() => {
            window.location.reload();
          }}
          className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
        >
          Reload page
        </button>
        <button
          onClick={logOut}
          className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
        >
          Log out
        </button>
      </div>
    </div>
  );
}

interface ErrorBoundaryState {
  error?: Error;
}

export class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  ErrorBoundaryState
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { error: undefined };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error("MainErrorBoundary caught error:", error, errorInfo);
  }

  render() {
    if (this.state.error) {
      return (
        <ErrorUI
          error={this.state.error}
          errorType={getJazzErrorType(this.state.error)}
        />
      );
    }

    return this.props.children;
  }
}
