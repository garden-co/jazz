/**
 * Native and wasm builds before the `jazz-auth-v1:` handover took the device
 * account secret as 64 hexadecimal characters, and refuse today's text with
 * these messages. Seeing one means the build is older than this package.
 */
const OLDER_BUILD_REFUSALS = [
  "a recovery secret is 64 hexadecimal characters",
  "a recovery secret is hexadecimal",
];

export const SECRET_FORMAT_MISMATCH =
  "The device account secret was refused by a Jazz native or wasm build that expects 64 hexadecimal characters: that build is older than this Jazz JavaScript package, which passes `jazz-auth-v1:` text. Install matching versions of the Jazz packages.";

/** Whether `error` is an older native build's refusal of a `jazz-auth-v1:` secret. */
export function isOlderBuildSecretRefusal(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return OLDER_BUILD_REFUSALS.some((refusal) => message.includes(refusal));
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
