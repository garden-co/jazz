import React from "react";
import { Button } from "@/components/ui/button";

function ErrorUI({ error }: { error: Error }) {
  console.error(error.stack);

  const message = error.message?.toLowerCase() ?? "";
  const isUnauthorised =
    message.includes("unauthorized") ||
    message.includes("permission") ||
    message.includes("access");
  const isDeleted = message.includes("deleted");
  const isUnavailable = message.includes("unavailable") || message.includes("not found");

  if (isUnauthorised) {
    return (
      <div className="flex flex-1 items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            You don't have access to this chat
          </h1>
          <Button
            onClick={() => {
              window.location.hash = "#/";
            }}
          >
            Go to home page
          </Button>
        </div>
      </div>
    );
  }

  if (isDeleted) {
    return (
      <div className="flex min-h-screen items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The chat you are trying to access has been permanently deleted
          </h1>
          <Button
            onClick={() => {
              window.location.hash = "#/";
            }}
          >
            Go to home page
          </Button>
        </div>
      </div>
    );
  }

  if (isUnavailable) {
    return (
      <div className="flex flex-1 items-center justify-center p-8">
        <div className="max-w-2xl space-y-4">
          <h1 className="text-2xl font-semibold text-red-600">
            The chat you are trying to access is unavailable.
          </h1>
          <p>
            This means either the chat does not exist or it is not available to you at the moment.
          </p>
          <Button
            onClick={() => {
              window.location.hash = "#/";
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
        <h1 className="text-2xl font-semibold text-red-600">Something went wrong</h1>
        <p className="text-muted-foreground">{error.message || "An unexpected error occurred"}</p>
        <Button
          onClick={() => {
            window.location.reload();
          }}
        >
          Reload page
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
    console.error("ErrorBoundary caught error:", error, errorInfo);
  }

  render() {
    if (this.state.error) {
      return <ErrorUI error={this.state.error} />;
    }
    return this.props.children;
  }
}
