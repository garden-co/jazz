import type { AccountDbConfig } from "../../src/accounts/context.js";
import type { AccountHandle } from "../../src/accounts/state.js";

// Mock client factories receive the handle but never enroll or open an account.
const account = {
  id: "test-account",
  identity: { issuer: "test", subject: "device" },
} as AccountHandle;

export function accountConfig(appId: string) {
  return { appId, account } satisfies AccountDbConfig;
}
