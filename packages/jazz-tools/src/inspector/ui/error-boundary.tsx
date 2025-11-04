import React from "react";
import { Text } from "./text";
import { styled } from "goober";

interface ErrorBoundaryState {
  hasError: boolean;
  error?: Error;
}

export class ErrorBoundary extends React.Component<
  { children: React.ReactNode; title: string },
  ErrorBoundaryState
> {
  constructor(props: { children: React.ReactNode; title: string }) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error(error);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: "1rem" }}>
          <StyledHeading>{this.props.title}</StyledHeading>
          <Text mono style={{ marginTop: "0.5rem", color: "#ef4444" }}>
            {this.state.error?.message || "An unexpected error occurred"}
          </Text>

          <pre style={{ paddingLeft: "1rem", color: "#ef4444" }}>
            {this.state.error?.stack}
          </pre>
        </div>
      );
    }

    return this.props.children;
  }
}

const StyledHeading = styled("h1")<{ className?: string }>`
  font-size: 1.125rem;
  font-weight: 500;
  color: var(--j-text-color-strong);
`;
