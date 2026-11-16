import { describe, expect, it } from "vitest";
import {
  COMMIT_OUTCOME_UNKNOWN,
  ThinCommitOutcomeUnknownError,
  ThinRemoteError,
  thinRemoteError,
} from "./thin-runtime.js";

describe("thin request errors", () => {
  it("throws a commit whose outcome is unknown as its own error, not as a failure", () => {
    const unknown = thinRemoteError(COMMIT_OUTCOME_UNKNOWN, "the core did not confirm the commit");
    expect(unknown).toBeInstanceOf(ThinCommitOutcomeUnknownError);
    expect(unknown).toBeInstanceOf(ThinRemoteError);
    expect(unknown.code).toBe("commit_outcome_unknown");
    expect(unknown.name).toBe("ThinCommitOutcomeUnknownError");

    const refused = thinRemoteError("write_rejected", "refused");
    expect(refused).not.toBeInstanceOf(ThinCommitOutcomeUnknownError);
    expect(refused.code).toBe("write_rejected");
  });
});
