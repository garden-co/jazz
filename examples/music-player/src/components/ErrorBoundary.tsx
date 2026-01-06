import React from "react";

interface ErrorBoundaryState {
  hasError: boolean;
  isAuthorizationError?: boolean;
  error?: Error;
}

interface ErrorBoundaryProps {
  children: React.ReactNode;
  fallback?: (error: Error) => React.ReactNode;
}

export class ErrorBoundary extends React.Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    if (error.message.includes("Jazz Authorization Error")) {
      return { hasError: true, isAuthorizationError: true, error };
    }

    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error("Error caught by boundary:", error, errorInfo);
  }

  render() {
    if (this.state.hasError && this.state.error) {
      if (this.props.fallback) {
        return this.props.fallback(this.state.error);
      }

      if (this.state.isAuthorizationError) {
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

      return (
        <div className="flex min-h-screen items-center justify-center p-8">
          <div className="max-w-2xl space-y-4">
            <h1 className="text-2xl font-semibold text-red-600">
              Something went wrong
            </h1>
            <p className="text-muted-foreground">
              {this.state.error.message || "An unexpected error occurred"}
            </p>
            {process.env.NODE_ENV === "development" && (
              <pre className="mt-4 overflow-auto rounded-md bg-muted p-4 text-sm">
                {this.state.error.stack}
              </pre>
            )}
            <button
              onClick={() => {
                this.setState({ hasError: false, error: undefined });
                window.location.reload();
              }}
              className="mt-4 rounded-md bg-primary px-4 py-2 text-primary-foreground hover:bg-primary/90"
            >
              Reload page
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
