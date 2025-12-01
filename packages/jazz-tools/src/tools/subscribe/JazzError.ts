import type { CoValue, ID } from "../internal.js";
import { CoValueLoadingState } from "./types.js";

export class JazzError {
  constructor(
    public id: ID<CoValue> | undefined,
    public type:
      | typeof CoValueLoadingState.UNAVAILABLE
      | typeof CoValueLoadingState.UNAUTHORIZED,
    public issues: JazzErrorIssue[],
  ) {}

  toString() {
    // Capture stack trace to help developers find where unavailable values are accessed
    const accessStack = new Error().stack || "";
    const stackLines = accessStack.split("\n").slice(2, 8); // Skip Error and toString lines

    // Build the main error message with inline stack info so it shows even if truncated
    let result = this.issues
      .map((issue) => {
        let message = `${issue.message}`;

        if (this.id) {
          message += ` from ${this.id}`;
        }

        if (issue.path.length > 0) {
          message += ` on path ${issue.path.join(".")}`;
        }

        // Add the most relevant stack frame inline
        const relevantFrame =
          stackLines.find(
            (line) =>
              line.includes("/packages/app/") && !line.includes("node_modules"),
          ) || stackLines[0];

        if (relevantFrame) {
          message += ` | Accessed at: ${relevantFrame.trim()}`;
        }

        return message;
      })
      .join("\n");

    // Add helpful diagnostic information
    result += "\n\n🔍 JAZZ DIAGNOSTIC INFO:";
    result += `\n  CoValue ID: ${this.id}`;
    result += `\n  Error Type: ${this.type}`;
    result += "\n  Access Stack:";
    for (const line of stackLines) {
      result += `\n    ${line.trim()}`;
    }
    result +=
      '\n\n💡 TIP: Check if you\'re accessing a field without first calling $jazz.has("field")';
    result +=
      "\n      or ensure you're using the value returned from ensureLoaded()";

    return result;
  }

  prependPath(item: string) {
    if (this.issues.length === 0) {
      return this;
    }

    const issues = this.issues.map((issue) => {
      return {
        ...issue,
        path: [item].concat(issue.path),
      };
    });

    return new JazzError(this.id, this.type, issues);
  }
}
export type JazzErrorIssue = {
  code:
    | typeof CoValueLoadingState.UNAVAILABLE
    | typeof CoValueLoadingState.UNAUTHORIZED
    | "validationError";
  message: string;
  params: Record<string, any>;
  path: string[];
};
