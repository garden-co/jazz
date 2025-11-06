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
    return this.issues
      .map((issue) => {
        let message = `${issue.message}`;

        if (this.id) {
          message += ` from ${this.id}`;
        }

        if (issue.path.length > 0) {
          message += ` on path ${issue.path.join(".")}`;
        }

        return message;
      })
      .join("\n");
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
