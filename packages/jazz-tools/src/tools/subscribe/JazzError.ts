import type { CoValue, CoValueErrorState, ID } from "../internal.js";
import { CoValueLoadingState } from "./types.js";

export class JazzError {
  constructor(
    public id: ID<CoValue> | undefined,
    public type:
      | typeof CoValueLoadingState.UNAVAILABLE
      | typeof CoValueLoadingState.DELETED
      | typeof CoValueLoadingState.UNAUTHORIZED,
    public issues: JazzErrorIssue[],
  ) {}

  toString() {
    // Build the main error message with inline stack info so it shows even if truncated
    let result = this.issues
      .map((issue) => {
        let message = `${issue.message}`;

        if (issue.path.length > 0) {
          if (this.id) {
            message += `. Subscription starts from ${this.id}`;
          }

          message += ` and the value is on path ${issue.path.join(".")}`;
        }

        return message;
      })
      .join("\n");

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
    | typeof CoValueLoadingState.DELETED
    | typeof CoValueLoadingState.UNAUTHORIZED
    | "validationError"
    | "deleteError";
  message: string;
  params: Record<string, any>;
  path: string[];
};

export function fillErrorWithJazzErrorInfo(
  /**
   * The error we are going to fill with the jazz error info.
   *
   * Passed externally to provide a better stack trace.
   */
  errorBase: Error,
  jazzError: JazzError | undefined,
): Error {
  if (!jazzError) {
    return errorBase;
  }

  errorBase.message = jazzError.toString();

  Object.defineProperty(errorBase, "@jazzErrorType", {
    value: jazzError.type,
  });

  return errorBase;
}

export function getJazzErrorType(
  error: unknown,
): CoValueErrorState | "unknown" {
  if (
    error instanceof Error &&
    "@jazzErrorType" in error &&
    typeof error["@jazzErrorType"] === "string"
  ) {
    return error["@jazzErrorType"] as CoValueErrorState;
  }

  return "unknown";
}
