/**
 * Native (napi) and wasm builds before the `jazz-auth-v1:` handover took the
 * device account secret as 64 hexadecimal characters, and refuse today's
 * text, which is never 64 characters long, with this message. Seeing it
 * means the build is older than this package. (Their other refusal, "a
 * recovery secret is hexadecimal", needs 64 characters, so it cannot occur.)
 *
 * React Native's relay is not covered: an older one refuses the text with a
 * bare `invalid_command` status, which a current relay also returns for any
 * request it cannot read, so the two cannot be told apart.
 */
const OLDER_BUILD_REFUSAL = "a recovery secret is 64 hexadecimal characters";

export const SECRET_FORMAT_MISMATCH =
  "The device account secret was refused by a Jazz native or wasm build that expects 64 hexadecimal characters: that build is older than this Jazz JavaScript package, which passes `jazz-auth-v1:` text. Install matching versions of the Jazz packages.";

/** Whether `error` is an older native build's refusal of a `jazz-auth-v1:` secret. */
export function isOlderBuildSecretRefusal(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes(OLDER_BUILD_REFUSAL);
}

/** `error`, or the explanation of an older native build's refusal of the secret. */
export function secretFormatExplained(error: unknown): unknown {
  return isOlderBuildSecretRefusal(error)
    ? new Error(SECRET_FORMAT_MISMATCH, { cause: error })
    : error;
}

/** Runs `run`, explaining an older native build's refusal of the secret. */
export function explainingSecretFormat<T>(run: () => T): T {
  try {
    return run();
  } catch (error) {
    throw secretFormatExplained(error);
  }
}
