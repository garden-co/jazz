// TypeScript users see a removed export too: it is `@deprecated` (naming the
// replacement), and a call with arguments doesn't type-check.
import { createAccountManager, useJazzAuth } from "./react/index.js";

export function removedExportsDontTypeCheckWithArguments() {
  // @ts-expect-error -- removed in alpha.61: use createJazzClient(config)
  createAccountManager({ serverUrl: "/jazz" });
  // Without arguments it type-checks, but returns `never`: nothing after it runs.
  const never: never = useJazzAuth();
  return never;
}
