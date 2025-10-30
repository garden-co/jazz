/**
 * Use your email as a temporary key, or get a free
 * API Key at dashboard.jazz.tools for higher limits.
 *
 * @link https://dashboard.jazz.tools
 */
const apiKey = "you@example.com";
// ---cut---
// @noErrors: 2307 7031 2304 2686 2664
// #region Basic
import { JazzExpoProvider } from "jazz-tools/expo";
import { MyAppAccount } from "./schema";

export function MyJazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <JazzExpoProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzExpoProvider>
  );
}
// #endregion
