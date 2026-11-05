import { Component, type ReactNode } from "react";
import { Button } from "./ui/button.js";

export class ErrorBoundary extends Component<{ children: ReactNode }, { error?: Error }> {
  state: { error?: Error } = {};
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error)
      return (
        <div className="p-8 space-y-4">
          <h1 className="text-xl">Unable to open encrypted chat</h1>
          <p role="alert">{this.state.error.message}</p>
          <p>Existing account keys are retained. Reload to explicitly retry.</p>
          <Button onClick={() => location.reload()}>Reload page</Button>
        </div>
      );
    return this.props.children;
  }
}
