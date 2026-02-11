import React from "react";
import { getJazzErrorType } from "jazz-tools";
import { useAccount, useLogOut } from "jazz-tools/react";
import { Button } from "@/components/ui/button";
import { ChatAccount } from "@/schema";

function ErrorUI({
  error,
  errorType,
}: {
  error: Error;
  errorType: ReturnType<typeof getJazzErrorType>;
}) {
  const logOut = useLogOut();
  const me = useAccount(ChatAccount, { resolve: { root: true } });
  console.error(error.stack);
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
          <Button onClick={logOut}>Log out</Button>
        </div>
      </div>
    );
  }

  if (errorType === "unauthorized") {
    return (
      <div className="flex flex-1 items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            You don't have access to this chat
          </h1>
          <Button
            onClick={() => {
              window.location.href = "/";
            }}
          >
            Go to home page
          </Button>
        </div>
      </div>
    );
  }

  if (errorType === "deleted") {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The chat you are trying to access has been permanently deleted
          </h1>
          <Button
            onClick={() => {
              window.location.href = "/";
            }}
          >
            Go to home page
          </Button>
        </div>
      </div>
    );
  }

  if (errorType === "unavailable") {
    return (
      <div className="flex flex-1 items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The chat you are trying to access is unavailable.
          </h1>
          <p>
            This means either the chat does not exist or it is not available to
            you at the moment. You may need to connect to the internet or try
            again later.
          </p>
          <Button
            onClick={() => {
              window.location.href = "/";
            }}
          >
            Go to home page
          </Button>
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

        <Button
          onClick={() => {
            window.location.reload();
          }}
        >
          Reload page
        </Button>
        <Button onClick={logOut} variant="destructive">
          Log out
        </Button>
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
