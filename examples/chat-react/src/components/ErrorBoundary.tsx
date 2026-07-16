import React from "react";
import { Button } from "@/components/ui/button";

type ErrorKind = "unauthorised" | "deleted" | "unavailable" | "unknown";

function classifyError(error: Error): ErrorKind {
  const message = error.message?.toLowerCase() ?? "";

  if (
    message.includes("unauthorized") ||
    message.includes("unauthorised") ||
    message.includes("permission") ||
    message.includes("not allowed") ||
    // The runtime surfaces policy failures as "policy denied <op> on table …".
    message.includes("denied")
  ) {
    return "unauthorised";
  }
  if (message.includes("deleted")) return "deleted";
  if (message.includes("unavailable") || message.includes("not found")) return "unavailable";
  return "unknown";
}

function ErrorUI({ error }: { error: Error }) {
  console.error(error.stack);

  const kind = classifyError(error);

  if (kind === "unauthorised") {
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

  if (kind === "deleted") {
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

  if (kind === "unavailable") {
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
