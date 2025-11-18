const apiKey = "you@example.com";
// #region Basic
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { MyAppAccount } from "./schema";

export function MyJazzProvider({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactNativeProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      AccountSchema={MyAppAccount}
    >
      {children}
    </JazzReactNativeProvider>
  );
}
// #endregion
